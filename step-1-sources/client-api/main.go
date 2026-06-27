// main.go — CLIENT API: CLI для клиентов маркетплейса.
//
// Предоставляет три команды:
//   search <запрос>    — поиск товаров по имени/описанию (локально в products.json)
//   recommend          — получить последние рекомендации из Kafka кластера 2
//   event <тип> <id>   — отправить событие (поиск, просмотр) в Kafka кластер 1
//
// Архитектура подключений:
//   Кластер 1 (kafka-1:9092,...) — PRODUCE events → client-events
//   Кластер 2 (kafka-analytics:9092) — CONSUME ← recommendations
//   Оба кластера: SASL/SCRAM-SHA-512 + TLS (одинаковый CA)
//
// Конфигурация через переменные окружения:
//   KAFKA_BROKERS_C1  — кластер 1, default: kafka-1:9092,kafka-2:9092,kafka-3:9092
//   KAFKA_BROKERS_C2  — кластер 2, default: kafka-analytics:9092
//   KAFKA_TLS_CA      — CA сертификат, default: ../ssl/certs/ca.crt
//   PRODUCTS_FILE     — каталог товаров (для search), default: ../shop-api/data/products.json
//
//   Учётные данные кластера 1:
//   KAFKA_USER_C1     — default: client-api
//   KAFKA_PASSWORD_C1 — default: ClientApiPass1
//
//   Учётные данные кластера 2:
//   KAFKA_USER_C2     — default: client-api
//   KAFKA_PASSWORD_C2 — default: ClientApiPass1
//
// Запуск:
//   go run . search "ноутбук"
//   go run . recommend
//   go run . event search prod-001
package main

import (
	// crypto/tls — TLS конфиг для шифрования трафика до брокеров
	"crypto/tls"
	// crypto/x509 — X.509 сертификаты: добавляем CA в пул доверенных
	"crypto/x509"
	// encoding/json — сериализация ClientEvent и десериализация Product
	"encoding/json"
	// fmt — форматированный вывод результатов команд
	"fmt"
	// log — логирование ошибок с временными метками
	"log"
	// os — переменные окружения, аргументы командной строки (os.Args)
	"os"
	// os/signal — перехват сигналов ОС (SIGINT = Ctrl+C, SIGTERM = kill)
	"os/signal"
	// strings — сравнение строк и разбивка списка брокеров
	"strings"
	// syscall — константы SIGINT и SIGTERM
	"syscall"
	// time — временные метки для событий
	"time"

	// IBM/sarama — Kafka-клиент для Go
	"github.com/IBM/sarama"
	// kafkascram — адаптер SCRAM (см. scram/scram.go)
	kafkascram "client-api/scram"
)

// Product — структура товара (та же что в shop-api, для поиска локально).
// Используется при команде "search" для десериализации products.json.
type Product struct {
	// Идентификатор товара
	ID string `json:"id"`
	// Название товара
	Name string `json:"name"`
	// Категория (например: electronics, books, tobacco)
	Category string `json:"category"`
	// Описание
	Description string `json:"description"`
	// Цена
	Price float64 `json:"price"`
	// Валюта
	Currency string `json:"currency"`
	// Остаток
	Quantity int `json:"quantity"`
	// ID продавца
	SellerID string `json:"seller_id"`
	// Бренд
	Brand string `json:"brand"`
	// Теги для поиска
	Tags []string `json:"tags"`
	// Дата добавления
	CreatedAt string `json:"created_at"`
}

// ClientEvent — событие клиента (поиск, просмотр товара, клик).
// Публикуется в топик client-events кластера 1 для аналитики.
// В Шаге 3 эти события анализируются для формирования рекомендаций.
type ClientEvent struct {
	// Уникальный ID события (используем timestamp + тип как псевдо-UUID)
	EventID string `json:"event_id"`
	// Тип события: "search", "view", "click", "purchase"
	EventType string `json:"event_type"`
	// ID пользователя (упрощённо — фиксированный для учебного проекта)
	UserID string `json:"user_id"`
	// ID товара (может быть пустым для события "search")
	ProductID string `json:"product_id"`
	// Поисковый запрос (для события "search")
	Query string `json:"query"`
	// Временная метка события (RFC3339)
	Timestamp string `json:"timestamp"`
}

// Recommendation — рекомендация от Spark (читается из топика recommendations кластера 2).
// Spark (Шаг 3) записывает JSON с рекомендованными товарами для пользователя.
type Recommendation struct {
	// ID пользователя которому адресована рекомендация
	UserID string `json:"user_id"`
	// Список рекомендованных товаров
	ProductIDs []string `json:"product_ids"`
	// Временная метка когда была сформирована рекомендация
	GeneratedAt string `json:"generated_at"`
	// Метаданные (например: алгоритм, параметры модели)
	Metadata map[string]string `json:"metadata"`
}

// getEnv возвращает значение переменной окружения или defaultValue.
func getEnv(key, defaultValue string) string {
	// os.Getenv возвращает "" если переменная не задана
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// buildTLSConfig создаёт TLS конфигурацию с нашим CA.
// Используется для обоих кластеров (один CA подписывает все сертификаты).
func buildTLSConfig(caFile string) (*tls.Config, error) {
	// Читаем CA сертификат из файла
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		// Возвращаем ошибку вызывающему коду для нормальной обработки
		return nil, fmt.Errorf("не удалось прочитать CA %s: %w", caFile, err)
	}

	// Создаём изолированный пул доверенных CA (не берём системные CA)
	caCertPool := x509.NewCertPool()
	// AppendCertsFromPEM парсит PEM и добавляет сертификаты в пул
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("не удалось добавить CA в пул — проверьте PEM формат")
	}

	// tls.Config: только наш CA для верификации сертификата брокера
	return &tls.Config{RootCAs: caCertPool}, nil
}

// buildSaramaConfig создаёт sarama.Config с SASL/SCRAM-SHA-512 и TLS.
// Параметры: user/password — учётные данные, tlsCfg — TLS конфиг.
func buildSaramaConfig(user, password string, tlsCfg *tls.Config) *sarama.Config {
	// NewConfig — конфиг с разумными дефолтами
	cfg := sarama.NewConfig()

	// Версия протокола Kafka 3.7 (sarama V3_6_0_0 совместима)
	cfg.Version = sarama.V3_6_0_0

	// ── SASL/SCRAM-SHA-512 ────────────────────────────────────────────────────

	// Enable=true — включаем SASL аутентификацию
	cfg.Net.SASL.Enable = true
	// SCRAM-SHA-512 — механизм заданный в docker-compose.yml (KAFKA_CFG_SASL_ENABLED_MECHANISMS)
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	// Учётные данные пользователя (client-api в нашем случае)
	cfg.Net.SASL.User = user
	cfg.Net.SASL.Password = password
	// Handshake=true — SASL Handshake v1 (стандарт для Kafka)
	cfg.Net.SASL.Handshake = true
	// Фабрика SCRAM-клиентов — возвращает новый экземпляр на каждое соединение
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// ── TLS ───────────────────────────────────────────────────────────────────

	// Enable=true — включаем TLS шифрование
	cfg.Net.TLS.Enable = true
	// Используем подготовленный TLS-конфиг с нашим CA
	cfg.Net.TLS.Config = tlsCfg

	return cfg
}

// publishSearchEvent публикует поисковый запрос в топик client-events кластера 1.
// Вызывается из cmdSearch после локального поиска — для аналитики в PySpark.
// Ошибки Kafka не прерывают поиск: warn-лог, результаты пользователю уже показаны.
func publishSearchEvent(query string) {
	// Кластер 1 — основной, туда поступают все клиентские события
	brokersStr := getEnv("KAFKA_BROKERS_C1", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
	brokers := strings.Split(brokersStr, ",")

	// Учётные данные client-api в кластере 1
	user := getEnv("KAFKA_USER_C1", "client-api")
	password := getEnv("KAFKA_PASSWORD_C1", "ClientApiPass1")

	// CA сертификат общий для обоих кластеров
	caFile := getEnv("KAFKA_TLS_CA", "../ssl/certs/ca.crt")

	// Строим TLS конфиг
	tlsCfg, err := buildTLSConfig(caFile)
	if err != nil {
		// Поиск уже выполнен — не прерываем, только логируем
		log.Printf("WARN: TLS ошибка, событие поиска не отправлено: %v", err)
		return
	}

	// Строим sarama конфиг с SASL/SCRAM-SHA-512
	cfg := buildSaramaConfig(user, password, tlsCfg)
	cfg.Producer.RequiredAcks = sarama.WaitForAll   // Ждём подтверждения всех ISR
	cfg.Producer.Idempotent = true                   // Защита от дублирования при ретраях
	cfg.Net.MaxOpenRequests = 1                      // Обязательно при Idempotent=true
	cfg.Producer.Return.Successes = true             // SyncProducer требует Successes=true
	cfg.Producer.Return.Errors = true                // SyncProducer требует Errors=true

	// Создаём синхронный продюсер
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Printf("WARN: не удалось подключиться к Kafka, событие поиска не отправлено: %v", err)
		return
	}
	defer producer.Close()

	// Временная метка в RFC3339 (стандарт для аналитики в Spark)
	now := time.Now().UTC()

	// Псевдо-ID события: тип + Unix timestamp в наносекундах
	eventID := fmt.Sprintf("search-%d", now.UnixNano())

	// User ID — в учебном проекте заглушка, в продакшне из JWT/сессии
	userID := getEnv("CLIENT_USER_ID", "user-demo-001")

	// Формируем событие поиска
	event := ClientEvent{
		EventID:   eventID,                  // Уникальный идентификатор
		EventType: "search",                 // Тип события
		UserID:    userID,                   // Кто искал
		ProductID: "",                       // Нет конкретного товара у поискового запроса
		Query:     query,                    // Что искали
		Timestamp: now.Format(time.RFC3339), // Когда произошло событие
	}

	// Сериализуем в JSON
	value, err := json.Marshal(event)
	if err != nil {
		log.Printf("WARN: ошибка сериализации события поиска: %v", err)
		return
	}

	// Ключ = user_id: все события одного пользователя → одна партиция (порядок гарантирован)
	msg := &sarama.ProducerMessage{
		Topic: "client-events",
		Key:   sarama.StringEncoder(userID),
		Value: sarama.ByteEncoder(value),
	}

	// Синхронная отправка с ожиданием ACK от всех ISR
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("WARN: ошибка отправки события поиска в Kafka: %v", err)
		return
	}

	// Подтверждение в лог (не в stdout — не мешает выводу результатов поиска)
	log.Printf("Событие поиска отправлено: query=%q → client-events partition=%d offset=%d",
		query, partition, offset)
}

// cmdSearch — команда поиска товаров по ключевому слову.
// Ищет локально по products.json И отправляет запрос в Kafka для аналитики (PySpark).
// query — поисковый запрос (проверяет name, description, tags, category).
func cmdSearch(query string) {
	// Путь к каталогу товаров — products.json создан в shop-api/data/
	productsFile := getEnv("PRODUCTS_FILE", "../shop-api/data/products.json")

	// Читаем файл в память
	data, err := os.ReadFile(productsFile)
	if err != nil {
		log.Fatalf("Не удалось прочитать каталог товаров %s: %v", productsFile, err)
	}

	// Декодируем JSON в срез Product
	var products []Product
	if err := json.Unmarshal(data, &products); err != nil {
		log.Fatalf("Не удалось разобрать JSON: %v", err)
	}

	// Нижний регистр запроса для регистронезависимого поиска
	queryLower := strings.ToLower(query)

	fmt.Printf("Поиск %q:\n\n", query)

	// Счётчик найденных товаров
	found := 0

	for _, p := range products {
		// Проверяем вхождение запроса в основные текстовые поля
		// strings.Contains — простой поиск подстроки (без индексов, для учебы достаточно)
		nameMatch := strings.Contains(strings.ToLower(p.Name), queryLower)
		descMatch := strings.Contains(strings.ToLower(p.Description), queryLower)
		catMatch := strings.Contains(strings.ToLower(p.Category), queryLower)
		brandMatch := strings.Contains(strings.ToLower(p.Brand), queryLower)

		// Проверяем теги (срез строк)
		tagMatch := false
		for _, tag := range p.Tags {
			// Если хоть один тег содержит запрос — считаем совпадением
			if strings.Contains(strings.ToLower(tag), queryLower) {
				tagMatch = true
				break // Нашли — нет смысла проверять остальные теги
			}
		}

		// Если хотя бы одно поле совпало — выводим товар
		if nameMatch || descMatch || catMatch || brandMatch || tagMatch {
			found++
			// Форматированный вывод результата поиска
			fmt.Printf("  [%s] %s\n", p.ID, p.Name)
			fmt.Printf("       Категория: %s | Бренд: %s | Цена: %.2f %s\n", p.Category, p.Brand, p.Price, p.Currency)
			fmt.Printf("       %s\n\n", p.Description)
		}
	}

	if found == 0 {
		// Сообщаем пользователю что ничего не найдено
		fmt.Printf("По запросу %q товары не найдены.\n", query)
		fmt.Println("Подсказка: в Шаге 5 будет подключён Elasticsearch с полнотекстовым поиском.")
	} else {
		fmt.Printf("Найдено: %d товар(ов)\n", found)
	}

	// Публикуем событие поиска в Kafka для аналитики в PySpark (Шаг 3).
	// Выполняется после вывода результатов — ошибки Kafka не блокируют пользователя.
	publishSearchEvent(query)
}

// cmdRecommend — команда получения рекомендаций из Kafka кластера 2.
// Читает несколько последних сообщений из топика recommendations.
// Топик заполняется PySpark-задачей в Шаге 3.
func cmdRecommend() {
	// ── Конфигурация подключения к кластеру 2 ─────────────────────────────────

	// Кластер 2 — аналитический, хранит рекомендации от Spark
	brokersStr := getEnv("KAFKA_BROKERS_C2", "kafka-analytics:9092")
	brokers := strings.Split(brokersStr, ",")

	// Учётные данные client-api в кластере 2 (созданы в cluster-2/docker-compose.yml)
	user := getEnv("KAFKA_USER_C2", "client-api")
	password := getEnv("KAFKA_PASSWORD_C2", "ClientApiPass1")

	// Путь к CA сертификату (одинаков для обоих кластеров — один PKI)
	caFile := getEnv("KAFKA_TLS_CA", "../ssl/certs/ca.crt")

	// Топик рекомендаций в кластере 2 (создан в cluster-2/setup.sh)
	topic := "recommendations"

	// ── Kafka конфигурация ─────────────────────────────────────────────────────

	// Строим TLS-конфиг с нашим CA
	tlsCfg, err := buildTLSConfig(caFile)
	if err != nil {
		log.Fatalf("TLS ошибка: %v", err)
	}

	// Строим sarama-конфиг с SASL и TLS
	cfg := buildSaramaConfig(user, password, tlsCfg)

	// Настройки consumer
	// OffsetNewest — читаем только новые сообщения (с момента подписки)
	// Для учебного проекта: если рекомендаций нет, будем ждать их появления
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	// ── Создаём consumer ───────────────────────────────────────────────────────

	// NewConsumer — low-level consumer, читаем конкретные партиции напрямую
	// Подходит для простого чтения "последних N сообщений"
	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		log.Fatalf("Не удалось подключиться к кластеру 2 (%s): %v\nПодсказка: проверьте что cluster-2 запущен и /etc/hosts содержит kafka-analytics", brokersStr, err)
	}
	defer consumer.Close()

	fmt.Println("Рекомендации из Kafka (кластер 2, топик: recommendations):")
	fmt.Println("Ожидаю новые рекомендации (Ctrl+C для выхода)...")
	fmt.Println()

	// Получаем метаданные топика: узнаём список партиций
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		// Топик может не существовать если Spark ещё не запускался (Шаг 3 не выполнен)
		fmt.Printf("Топик %q не найден: %v\n", topic, err)
		fmt.Println("Подсказка: рекомендации появятся после выполнения Шага 3 (PySpark).")
		return
	}

	// ── Подписываемся на каждую партицию ──────────────────────────────────────

	// Сообщения из всех партиций объединяем в один канал
	messages := make(chan *sarama.ConsumerMessage, 100)

	for _, partition := range partitions {
		// Создаём consumer для конкретной партиции, начиная с OffsetNewest
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Не удалось подписаться на партицию %d: %v", partition, err)
			continue
		}
		defer pc.Close()

		// Горутина читает сообщения из партиции и отправляет в общий канал
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				messages <- msg
			}
		}(pc)
	}

	// ── Читаем и отображаем рекомендации ──────────────────────────────────────

	// Перехватываем Ctrl+C / kill для завершения цикла чтения
	sigChan := make(chan os.Signal, 1)
	// signal.Notify регистрирует sigChan как получателя сигналов ОС
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	recvCount := 0 // Счётчик полученных рекомендаций

	// select смотрит оба канала одновременно:
	//   messages — новое сообщение из Kafka
	//   sigChan  — Ctrl+C или kill → выход из функции
	for {
		select {
		case msg, ok := <-messages:
			// ok=false означает что канал закрыт (consumer завершился)
			if !ok {
				return
			}
			recvCount++

			// Пытаемся десериализовать сообщение как Recommendation
			var rec Recommendation
			if err := json.Unmarshal(msg.Value, &rec); err != nil {
				// Если Spark пишет в другом формате — показываем raw JSON
				fmt.Printf("[%d] Сообщение (partition=%d offset=%d):\n  %s\n\n",
					recvCount, msg.Partition, msg.Offset, string(msg.Value))
				continue
			}

			// Выводим рекомендации в читаемом формате
			fmt.Printf("[%d] Рекомендации для пользователя %s (генерировано: %s):\n",
				recvCount, rec.UserID, rec.GeneratedAt)
			for i, productID := range rec.ProductIDs {
				// i+1 — порядковый номер в списке рекомендаций
				fmt.Printf("  %d. %s\n", i+1, productID)
			}
			fmt.Println()

		case sig := <-sigChan:
			// Пришёл сигнал завершения — выходим корректно
			fmt.Printf("\nПолучен сигнал %v, завершаю...\n", sig)
			return
		}
	}
}

// cmdEvent — команда отправки пользовательского события в Kafka кластер 1.
// eventType — тип события: "search", "view", "click", "purchase".
// productID — ID товара (может быть пустым для события "search").
func cmdEvent(eventType, productID string) {
	// ── Конфигурация подключения к кластеру 1 ─────────────────────────────────

	// Кластер 1 — основной, принимает все транзакционные события
	brokersStr := getEnv("KAFKA_BROKERS_C1", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
	brokers := strings.Split(brokersStr, ",")

	// Учётные данные client-api в кластере 1
	user := getEnv("KAFKA_USER_C1", "client-api")
	password := getEnv("KAFKA_PASSWORD_C1", "ClientApiPass1")

	// CA сертификат
	caFile := getEnv("KAFKA_TLS_CA", "../ssl/certs/ca.crt")

	// Топик client-events создан в cluster-1/setup.sh
	topic := "client-events"

	// ── Kafka конфигурация ─────────────────────────────────────────────────────

	tlsCfg, err := buildTLSConfig(caFile)
	if err != nil {
		log.Fatalf("TLS ошибка: %v", err)
	}

	cfg := buildSaramaConfig(user, password, tlsCfg)

	// Для producer нужны эти настройки
	cfg.Producer.RequiredAcks = sarama.WaitForAll   // Ждём подтверждения от всех ISR
	cfg.Producer.Idempotent = true                   // Защита от дублирования
	cfg.Net.MaxOpenRequests = 1                      // Обязательно при Idempotent=true
	cfg.Producer.Return.Successes = true             // Возвращать результат
	cfg.Producer.Return.Errors = true                // Возвращать ошибки

	// ── Создаём producer ───────────────────────────────────────────────────────

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Не удалось подключиться к кластеру 1: %v", err)
	}
	defer producer.Close()

	// ── Формируем событие ─────────────────────────────────────────────────────

	// Временная метка в RFC3339 формате (обязательна для аналитики)
	now := time.Now().UTC()

	// Псевдо-ID события: тип + Unix timestamp в наносекундах (простой способ без UUID)
	eventID := fmt.Sprintf("%s-%d", eventType, now.UnixNano())

	// Заглушка: в реальном приложении user_id приходит из сессии/токена
	userID := getEnv("CLIENT_USER_ID", "user-demo-001")

	event := ClientEvent{
		EventID:   eventID,          // Уникальный ID события
		EventType: eventType,        // Тип: search/view/click/purchase
		UserID:    userID,           // Кто совершил действие
		ProductID: productID,        // Какой товар (пустой для search)
		Query:     "",               // Поисковый запрос (заполняется только для search)
		Timestamp: now.Format(time.RFC3339), // Когда произошло событие
	}

	// Для события "search" поле query = productID (передаём текст запроса как productID)
	if eventType == "search" {
		event.Query = productID // При search: второй аргумент = текст запроса
		event.ProductID = ""    // ProductID не применим для поиска
	}

	// Сериализуем событие в JSON
	value, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("Ошибка сериализации события: %v", err)
	}

	// ── Отправляем событие ─────────────────────────────────────────────────────

	msg := &sarama.ProducerMessage{
		Topic: topic,
		// Ключ = user_id: события одного пользователя попадают в одну партицию
		// Это обеспечивает порядок событий для аналитики поведения пользователя
		Key:   sarama.StringEncoder(userID),
		Value: sarama.ByteEncoder(value),
	}

	// SendMessage — синхронная отправка с ожиданием ACK
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Ошибка отправки события: %v", err)
	}

	// Подтверждение отправки пользователю
	fmt.Printf("Событие отправлено:\n")
	fmt.Printf("  ID:        %s\n", event.EventID)
	fmt.Printf("  Тип:       %s\n", event.EventType)
	fmt.Printf("  Пользователь: %s\n", event.UserID)
	if event.ProductID != "" {
		fmt.Printf("  Товар:     %s\n", event.ProductID)
	}
	if event.Query != "" {
		fmt.Printf("  Запрос:    %s\n", event.Query)
	}
	fmt.Printf("  Топик:     %s (partition=%d offset=%d)\n", topic, partition, offset)
}

func main() {
	// os.Args — аргументы командной строки.
	// os.Args[0] = имя программы, os.Args[1] = команда, os.Args[2+] = аргументы команды.
	if len(os.Args) < 2 {
		// Выводим справку если команда не указана
		printUsage()
		os.Exit(1)
	}

	// os.Args[1] — название команды
	command := os.Args[1]

	switch command {
	case "search":
		// Команда: client-api search <запрос>
		if len(os.Args) < 3 {
			// Запрос обязателен
			fmt.Println("Использование: client-api search <запрос>")
			fmt.Println("Пример:        client-api search ноутбук")
			os.Exit(1)
		}
		// Объединяем все слова запроса в одну строку (поддержка многословных запросов)
		query := strings.Join(os.Args[2:], " ")
		cmdSearch(query)

	case "recommend":
		// Команда: client-api recommend
		// Нет дополнительных аргументов — просто читаем из Kafka
		cmdRecommend()

	case "event":
		// Команда: client-api event <тип> <product_id или запрос>
		if len(os.Args) < 4 {
			fmt.Println("Использование: client-api event <тип> <product_id>")
			fmt.Println("Типы событий:  search, view, click, purchase")
			fmt.Println("Примеры:")
			fmt.Println("  client-api event view prod-001")
			fmt.Println("  client-api event search ноутбук")
			os.Exit(1)
		}
		eventType := os.Args[2]  // Тип события
		productID := os.Args[3]  // ID товара или текст поиска
		cmdEvent(eventType, productID)

	default:
		// Неизвестная команда
		fmt.Printf("Неизвестная команда: %q\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

// printUsage выводит справку по использованию CLIENT API.
func printUsage() {
	// Многострочный литерал (backtick) — удобен для форматированного вывода
	fmt.Println(`CLIENT API — клиент маркетплейса "Покупай выгодно"

Использование:
  client-api <команда> [аргументы]

Команды:
  search <запрос>        Поиск товаров по названию, описанию или тегам
  recommend              Получить рекомендации из Kafka (кластер 2)
  event <тип> <id>       Отправить событие в Kafka (кластер 1)

Примеры:
  client-api search "ноутбук"
  client-api search электроника
  client-api recommend
  client-api event view prod-001
  client-api event search "смартфон самсунг"
  client-api event click prod-002

Переменные окружения:
  KAFKA_BROKERS_C1   Кластер 1 (default: kafka-1:9092,kafka-2:9092,kafka-3:9092)
  KAFKA_BROKERS_C2   Кластер 2 (default: kafka-analytics:9092)
  KAFKA_TLS_CA       Путь к CA сертификату (default: ../ssl/certs/ca.crt)

Примечание: добавьте в /etc/hosts:
  127.0.0.1 kafka-1 kafka-2 kafka-3 kafka-analytics`)
}
