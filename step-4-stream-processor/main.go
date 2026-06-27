// main.go — Stream Processor на базе Goka: потоковая фильтрация товаров.
//
// Читает сообщения из топика products-raw (кластер 1), проверяет категорию товара
// по списку запрещённых и пишет только разрешённые товары в products-filtered.
//
// Goka — высокоуровневый фреймворк потоковой обработки поверх sarama.
// В отличие от чистого sarama ConsumerGroup, Goka:
//   - управляет rebалансировкой автоматически
//   - декларативно описывает граф обработки (DefineGroup)
//   - встраивает producer в процессор (ctx.Emit вместо отдельного SyncProducer)
//
// CLI команды (управление списком запрещённых категорий):
//   go run .                  — запустить процессор (аналог go run . process)
//   go run . process          — запустить процессор явно
//   go run . list             — показать текущий список запрещённых категорий
//   go run . add <категория>  — добавить категорию в список
//   go run . remove <категория> — удалить категорию из списка
//
// Список категорий хранится в banned_categories.json в рабочей директории.
//
// Конфигурация через переменные окружения:
//   KAFKA_BROKERS   — брокеры кластера 1, default: kafka-1:9092,kafka-2:9092,kafka-3:9092
//   KAFKA_USER      — default: stream-processor
//   KAFKA_PASSWORD  — default: StreamPass1
//   KAFKA_TLS_CA    — путь к CA сертификату, default: ../step-2-kafka/ssl/certs/ca.crt
//   TOPIC_IN        — входной топик, default: products-raw
//   TOPIC_OUT       — выходной топик, default: products-filtered
//   GROUP_ID        — consumer group, default: stream-processor
package main

import (
	// context — управление жизненным циклом процессора (отмена при SIGINT)
	"context"
	// crypto/tls — TLS конфигурация для подключения к брокерам
	"crypto/tls"
	// crypto/x509 — загрузка CA сертификата в пул доверенных
	"crypto/x509"
	// encoding/json — сериализация/десериализация Product и banned_categories.json
	"encoding/json"
	// fmt — вывод в stdout для CLI команд (list, add, remove)
	"fmt"
	// log — логирование событий обработки (PASSED/FILTERED) с меткой времени
	"log"
	// os — чтение файлов, переменных окружения, os.Args для CLI
	"os"
	// os/signal — перехват SIGINT/SIGTERM для graceful shutdown
	"os/signal"
	// sort — сортировка категорий при выводе списка
	"sort"
	// strings — разбивка строки брокеров по запятой
	"strings"
	// syscall — константы SIGINT, SIGTERM
	"syscall"

	// IBM/sarama — Kafka-клиент для Go; Goka использует его внутри
	"github.com/IBM/sarama"
	// lovoo/goka — фреймворк потоковой обработки поверх sarama
	// Предоставляет DefineGroup, Processor, Context.Emit вместо ручного ConsumerGroup
	"github.com/lovoo/goka"
	// kafkascram — локальный адаптер SCRAM-SHA-512 для sarama (scram/scram.go)
	kafkascram "stream-processor/scram"
)

// bannedFile — имя файла для хранения списка запрещённых категорий.
// Файл создаётся автоматически при первом запуске с дефолтным списком.
const bannedFile = "banned_categories.json"

// Product — структура товара из топика products-raw.
// Должна совпадать со структурой в shop-api/main.go.
type Product struct {
	// Уникальный идентификатор товара (например, prod-001)
	ID string `json:"id"`
	// Название товара
	Name string `json:"name"`
	// Категория — ключевое поле для фильтрации (tobacco, alcohol, weapons)
	Category string `json:"category"`
	// Описание товара
	Description string `json:"description"`
	// Цена в рублях
	Price float64 `json:"price"`
	// Валюта (всегда RUB)
	Currency string `json:"currency"`
	// Остаток на складе
	Quantity int `json:"quantity"`
	// ID продавца
	SellerID string `json:"seller_id"`
	// Бренд
	Brand string `json:"brand"`
	// Теги для поиска
	Tags []string `json:"tags"`
	// Дата создания (ISO 8601)
	CreatedAt string `json:"created_at"`
}

// ProductCodec реализует интерфейс goka.Codec для типа *Product.
//
// Goka требует кодек для каждого топика (Input/Output):
//   Encode — сериализация при записи в Kafka (ctx.Emit)
//   Decode — десериализация при чтении из Kafka (входящее сообщение → interface{})
type ProductCodec struct{}

// Encode сериализует *Product в JSON байты для записи в Kafka.
func (c *ProductCodec) Encode(value interface{}) ([]byte, error) {
	// json.Marshal — стандартная сериализация в JSON
	return json.Marshal(value)
}

// Decode десериализует JSON байты из Kafka в *Product.
// Возвращает interface{} — Goka передаёт его в ProcessCallback как msg interface{}.
func (c *ProductCodec) Decode(data []byte) (interface{}, error) {
	// Выделяем новый объект Product для каждого сообщения
	var p Product
	// Десериализуем JSON из Kafka value в структуру
	if err := json.Unmarshal(data, &p); err != nil {
		// Возвращаем ошибку — Goka пропустит сообщение и залогирует
		return nil, err
	}
	// Возвращаем указатель — в processFunc приводим msg.(*Product)
	return &p, nil
}

// loadBanned читает список запрещённых категорий из bannedFile.
// Если файл не существует — создаёт его с дефолтным списком.
func loadBanned() map[string]struct{} {
	// Читаем JSON файл со списком категорий
	data, err := os.ReadFile(bannedFile)
	if err != nil {
		// Файла нет — создаём дефолтный список и сохраняем его
		defaults := map[string]struct{}{
			// Табачная продукция запрещена на маркетплейсе
			"tobacco": {},
			// Алкогольные напитки запрещены
			"alcohol": {},
			// Оружие запрещено
			"weapons": {},
		}
		// Сохраняем дефолтный список чтобы пользователь мог его редактировать
		if err2 := saveBanned(defaults); err2 != nil {
			log.Printf("WARN: не удалось создать %s: %v", bannedFile, err2)
		}
		return defaults
	}

	// Файл содержит JSON массив строк: ["tobacco","alcohol","weapons"]
	var categories []string
	// Десериализуем массив строк
	if err := json.Unmarshal(data, &categories); err != nil {
		log.Fatalf("Ошибка парсинга %s: %v", bannedFile, err)
	}

	// Конвертируем в map для O(1) поиска при фильтрации
	result := make(map[string]struct{}, len(categories))
	for _, cat := range categories {
		// Добавляем каждую категорию как ключ (значение — пустая структура)
		result[cat] = struct{}{}
	}
	return result
}

// saveBanned сохраняет список запрещённых категорий в bannedFile.
// Категории сортируются для воспроизводимости (git diff).
func saveBanned(banned map[string]struct{}) error {
	// Извлекаем ключи из map в срез
	cats := make([]string, 0, len(banned))
	for cat := range banned {
		cats = append(cats, cat)
	}
	// Сортируем для стабильного порядка в файле
	sort.Strings(cats)

	// Сериализуем в JSON с отступами для читаемости
	data, err := json.MarshalIndent(cats, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}

	// Записываем файл с правами 0644 (чтение для всех, запись только владельцу)
	return os.WriteFile(bannedFile, data, 0644)
}

// cmdList выводит текущий список запрещённых категорий из bannedFile.
func cmdList() {
	// Загружаем список из файла
	banned := loadBanned()

	// Пустой список
	if len(banned) == 0 {
		fmt.Println("Список запрещённых категорий пуст.")
		return
	}

	// Извлекаем ключи и сортируем для удобного чтения
	cats := make([]string, 0, len(banned))
	for cat := range banned {
		cats = append(cats, cat)
	}
	sort.Strings(cats)

	// Выводим список
	fmt.Printf("Запрещённые категории (%s):\n", bannedFile)
	for _, cat := range cats {
		// Каждая категория на отдельной строке с отступом
		fmt.Printf("  - %s\n", cat)
	}
}

// cmdAdd добавляет категорию в список запрещённых.
func cmdAdd(category string) {
	// Приводим к нижнему регистру для унификации (tobacco = Tobacco)
	category = strings.ToLower(strings.TrimSpace(category))
	if category == "" {
		fmt.Fprintln(os.Stderr, "Ошибка: имя категории не может быть пустым.")
		os.Exit(1)
	}

	// Загружаем текущий список
	banned := loadBanned()

	// Проверяем, не существует ли уже
	if _, exists := banned[category]; exists {
		fmt.Printf("Категория '%s' уже в списке запрещённых.\n", category)
		return
	}

	// Добавляем категорию
	banned[category] = struct{}{}

	// Сохраняем обновлённый список
	if err := saveBanned(banned); err != nil {
		log.Fatalf("Ошибка сохранения: %v", err)
	}

	// Подтверждаем пользователю
	fmt.Printf("Категория '%s' добавлена в список запрещённых.\n", category)
	fmt.Printf("Перезапустите процессор чтобы изменения вступили в силу.\n")
}

// cmdRemove удаляет категорию из списка запрещённых.
func cmdRemove(category string) {
	// Приводим к нижнему регистру для унификации
	category = strings.ToLower(strings.TrimSpace(category))
	if category == "" {
		fmt.Fprintln(os.Stderr, "Ошибка: имя категории не может быть пустым.")
		os.Exit(1)
	}

	// Загружаем текущий список
	banned := loadBanned()

	// Проверяем наличие категории
	if _, exists := banned[category]; !exists {
		fmt.Printf("Категория '%s' не найдена в списке запрещённых.\n", category)
		return
	}

	// Удаляем категорию
	delete(banned, category)

	// Сохраняем обновлённый список
	if err := saveBanned(banned); err != nil {
		log.Fatalf("Ошибка сохранения: %v", err)
	}

	// Подтверждаем пользователю
	fmt.Printf("Категория '%s' удалена из списка запрещённых.\n", category)
	fmt.Printf("Перезапустите процессор чтобы изменения вступили в силу.\n")
}

// getEnv возвращает значение переменной окружения или defaultValue.
func getEnv(key, defaultValue string) string {
	// Проверяем, задана ли переменная и не пуста ли она
	if v := os.Getenv(key); v != "" {
		return v
	}
	// Возвращаем значение по умолчанию
	return defaultValue
}

// buildSaramaConfig создаёт sarama.Config с SASL/SCRAM-SHA-512 и TLS.
// Этот конфиг передаётся Goka через опции WithConsumerGroupBuilder и WithProducerBuilder.
func buildSaramaConfig(user, password, caFile string) (*sarama.Config, error) {
	// Начинаем с дефолтных настроек sarama
	cfg := sarama.NewConfig()

	// Версия протокола Kafka (Kafka 3.7 совместима с V3_6_0_0)
	cfg.Version = sarama.V3_6_0_0

	// ── TLS ───────────────────────────────────────────────────────────────────

	// Читаем CA сертификат для верификации брокеров
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать CA %s: %w", caFile, err)
	}

	// Создаём изолированный пул — доверяем только нашему CA
	pool := x509.NewCertPool()
	// Парсим PEM и добавляем в пул доверенных
	if ok := pool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("не удалось добавить CA в пул — проверьте PEM формат")
	}

	// Включаем TLS
	cfg.Net.TLS.Enable = true
	// Используем наш пул CA (не системный)
	cfg.Net.TLS.Config = &tls.Config{RootCAs: pool}

	// ── SASL/SCRAM-SHA-512 ────────────────────────────────────────────────────

	// Включаем SASL аутентификацию
	cfg.Net.SASL.Enable = true
	// Механизм SCRAM-SHA-512 (задан в docker-compose.yml кластера)
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	// Учётные данные сервиса stream-processor
	cfg.Net.SASL.User = user
	cfg.Net.SASL.Password = password
	// Handshake v1 — стандарт для современных версий Kafka
	cfg.Net.SASL.Handshake = true
	// Фабрика SCRAM-клиентов (реализована в scram/scram.go)
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// ── Producer настройки (для внутреннего продюсера Goka) ───────────────────

	// Goka требует возврата успехов и ошибок от продюсера
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	// Ждём подтверждения от всех ISR реплик — надёжная запись
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// ── Consumer настройки ────────────────────────────────────────────────────

	// При первом запуске читать с начала топика (не пропускать уже накопленные товары)
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	return cfg, nil
}

// cmdProcess запускает Goka-процессор потоковой фильтрации.
// Читает из TOPIC_IN, фильтрует по banned_categories.json, пишет в TOPIC_OUT.
func cmdProcess() {
	// ── Конфигурация из переменных окружения ──────────────────────────────────

	// Брокеры кластера 1 в виде строки с запятыми → срез
	brokersStr := getEnv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
	brokers := strings.Split(brokersStr, ",")

	// Учётные данные сервиса stream-processor
	user := getEnv("KAFKA_USER", "stream-processor")
	password := getEnv("KAFKA_PASSWORD", "StreamPass1")

	// Путь к CA сертификату для TLS верификации
	caFile := getEnv("KAFKA_TLS_CA", "../step-2-kafka/ssl/certs/ca.crt")

	// Входной топик (все товары от shop-api)
	topicIn := goka.Stream(getEnv("TOPIC_IN", "products-raw"))
	// Выходной топик (только разрешённые товары)
	topicOut := goka.Stream(getEnv("TOPIC_OUT", "products-filtered"))
	// Consumer group — уникальный идентификатор этого процессора
	group := goka.Group(getEnv("GROUP_ID", "stream-processor"))

	// ── Список запрещённых категорий ──────────────────────────────────────────

	// Загружаем список из файла (создаётся с дефолтами если не существует)
	bannedCategories := loadBanned()

	// Выводим загруженный список для подтверждения
	cats := make([]string, 0, len(bannedCategories))
	for cat := range bannedCategories {
		cats = append(cats, cat)
	}
	sort.Strings(cats)
	log.Printf("Stream Processor (Goka) запускается...")
	log.Printf("  Брокеры:      %s", brokersStr)
	log.Printf("  Вход:         %s", topicIn)
	log.Printf("  Выход:        %s", topicOut)
	log.Printf("  Группа:       %s", group)
	log.Printf("  Запрещены:    %s", strings.Join(cats, ", "))
	log.Printf("  Список файл:  %s", bannedFile)

	// ── Sarama конфигурация с SASL/TLS ────────────────────────────────────────

	// Строим sarama.Config с TLS и SCRAM-SHA-512
	saramaCfg, err := buildSaramaConfig(user, password, caFile)
	if err != nil {
		log.Fatalf("Ошибка конфигурации: %v", err)
	}

	// ── Функция обработки сообщений ───────────────────────────────────────────

	// processFunc вызывается Goka для каждого входящего сообщения из topicIn.
	// Goka гарантирует последовательный вызов для одного ключа (партиции).
	//
	// ctx — контекст Goka: содержит ключ, смещение, возможность Emit
	// msg — декодированное через ProductCodec сообщение (тип *Product)
	processFunc := func(ctx goka.Context, msg interface{}) {
		// Приводим interface{} к *Product (ProductCodec.Decode возвращает *Product)
		product, ok := msg.(*Product)
		if !ok {
			// Некорректный тип — пропускаем (не должно случиться при правильном кодеке)
			log.Printf("WARN: неверный тип сообщения (key=%s)", ctx.Key())
			return
		}

		// Проверяем категорию товара в множестве запрещённых (O(1))
		if _, banned := bannedCategories[product.Category]; banned {
			// Товар запрещён — не эмитируем, просто логируем
			// Не вызывая ctx.Emit, Goka не пишет ничего в topicOut
			log.Printf("FILTERED [%s] %s (category=%s) — запрещённая категория",
				product.ID, product.Name, product.Category)
			return
		}

		// Товар разрешён — передаём в products-filtered через ctx.Emit
		// ctx.Key() — ключ исходного сообщения (product.ID из shop-api)
		// topicOut — должен совпадать с Output-ребром в DefineGroup ниже
		ctx.Emit(topicOut, ctx.Key(), product)

		// Логируем успешную передачу для наблюдаемости
		log.Printf("PASSED  [%s] %s (category=%s) → %s",
			product.ID, product.Name, product.Category, topicOut)
	}

	// ── Граф потоковой обработки (Goka GroupGraph) ────────────────────────────

	// DefineGroup описывает топологию обработки декларативно:
	//   - Input:  подписка на topicIn, декодирование через ProductCodec, обработка в processFunc
	//   - Output: разрешаем emit в topicOut с кодированием через ProductCodec
	//
	// Goka сам создаст consumer group, настроит rebalancing и продюсер.
	g := goka.DefineGroup(
		group,  // имя consumer group (goka.Group — это string alias)
		// Input — входное ребро: топик + кодек + функция обработки
		goka.Input(topicIn, new(ProductCodec), processFunc),
		// Output — выходное ребро: топик + кодек (используется при ctx.Emit)
		goka.Output(topicOut, new(ProductCodec)),
	)

	// ── Создаём Goka Processor ────────────────────────────────────────────────

	// NewProcessor принимает адреса брокеров, граф и опции конфигурации.
	// ConsumerGroupBuilderWithConfig и ProducerBuilderWithConfig передают
	// наш sarama конфиг (с SASL/TLS) в consumer group и internal producer Goka.
	proc, err := goka.NewProcessor(
		brokers,
		g,
		// Передаём наш sarama конфиг в consumer group (SASL + TLS + OffsetOldest)
		goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(saramaCfg)),
		// Передаём наш sarama конфиг во внутренний producer Goka (SASL + TLS + acks=all)
		goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(saramaCfg)),
	)
	if err != nil {
		log.Fatalf("Не удалось создать Goka процессор: %v\n"+
			"Подсказка: проверьте что cluster-1 запущен и /etc/hosts настроен", err)
	}

	// ── Graceful Shutdown ──────────────────────────────────────────────────────

	// Контекст с возможностью отмены — передаём в proc.Run
	ctx, cancel := context.WithCancel(context.Background())
	// Канал для получения сигналов завершения от ОС
	sigChan := make(chan os.Signal, 1)
	// Подписываемся на Ctrl+C (SIGINT) и kill (SIGTERM)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Горутина: получив сигнал — отменяем контекст → proc.Run вернёт управление
	go func() {
		sig := <-sigChan
		log.Printf("Получен сигнал %v, завершаем работу...", sig)
		cancel()
	}()

	// ── Запуск процессора ─────────────────────────────────────────────────────

	// proc.Run — блокирующий вызов; возвращается только при отмене ctx или ошибке.
	// Внутри Goka запускает consumer group loop и обрабатывает rebalancing.
	if err := proc.Run(ctx); err != nil {
		log.Fatalf("Ошибка Goka процессора: %v", err)
	}
	log.Println("Stream Processor остановлен.")
}

func main() {
	// Определяем команду из первого аргумента командной строки
	// Если аргументов нет — запускаем процессор (режим по умолчанию)
	if len(os.Args) < 2 {
		// Режим по умолчанию: запуск процессора без аргументов
		cmdProcess()
		return
	}

	// Маршрутизация по первому аргументу
	switch os.Args[1] {

	case "process":
		// Явный запуск процессора
		cmdProcess()

	case "list":
		// Показать текущий список запрещённых категорий
		cmdList()

	case "add":
		// Добавить категорию: go run . add <категория>
		if len(os.Args) < 3 {
			// Категория не указана — показываем подсказку
			fmt.Fprintf(os.Stderr, "Использование: %s add <категория>\n", os.Args[0])
			fmt.Fprintf(os.Stderr, "Пример:        %s add cosmetics\n", os.Args[0])
			os.Exit(1)
		}
		cmdAdd(os.Args[2])

	case "remove":
		// Удалить категорию: go run . remove <категория>
		if len(os.Args) < 3 {
			// Категория не указана — показываем подсказку
			fmt.Fprintf(os.Stderr, "Использование: %s remove <категория>\n", os.Args[0])
			fmt.Fprintf(os.Stderr, "Пример:        %s remove weapons\n", os.Args[0])
			os.Exit(1)
		}
		cmdRemove(os.Args[2])

	default:
		// Неизвестная команда — показываем справку
		fmt.Fprintf(os.Stderr, "Неизвестная команда: %s\n\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Использование: %s [команда]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Команды:\n")
		fmt.Fprintf(os.Stderr, "  process             — запустить поток фильтрации (по умолчанию)\n")
		fmt.Fprintf(os.Stderr, "  list                — показать запрещённые категории\n")
		fmt.Fprintf(os.Stderr, "  add    <категория>  — добавить в список запрещённых\n")
		fmt.Fprintf(os.Stderr, "  remove <категория>  — удалить из списка запрещённых\n")
		os.Exit(1)
	}
}
