// main.go — Stream Processor: фильтрация товаров по категории.
//
// Читает сообщения из топика products-raw (кластер 1), проверяет категорию товара,
// и пишет только разрешённые товары в topics products-filtered (кластер 1).
//
// Товары с категориями tobacco, alcohol, weapons — отфильтровываются
// (не публикуются в products-filtered, только логируются).
//
// Архитектура:
//   Consumer group "stream-processor" ← products-raw (кластер 1)
//       ↓ фильтрация по category
//   SyncProducer → products-filtered (кластер 1)
//
// Конфигурация через переменные окружения:
//   KAFKA_BROKERS   — брокеры кластера 1, default: kafka-1:9092,kafka-2:9092,kafka-3:9092
//   KAFKA_USER      — default: stream-processor
//   KAFKA_PASSWORD  — default: StreamProcessorPass1
//   KAFKA_TLS_CA    — путь к CA сертификату, default: ../step-2-kafka/ssl/certs/ca.crt
//   TOPIC_IN        — входной топик, default: products-raw
//   TOPIC_OUT       — выходной топик, default: products-filtered
//   GROUP_ID        — consumer group, default: stream-processor
//
// Запуск:
//   go run . &
//   # запустить shop-api чтобы появились сообщения в products-raw
package main

import (
	// context — управление жизненным циклом consumer group (cancellation при SIGINT)
	"context"
	// crypto/tls — TLS конфиг для подключения к брокерам
	"crypto/tls"
	// crypto/x509 — добавляем CA в пул доверенных сертификатов
	"crypto/x509"
	// encoding/json — десериализация продуктов из products-raw, сериализация в products-filtered
	"encoding/json"
	// fmt — форматированный вывод лога фильтрации
	"fmt"
	// log — логирование ошибок с временными метками
	"log"
	// os — переменные окружения
	"os"
	// os/signal — перехват SIGINT/SIGTERM для graceful shutdown
	"os/signal"
	// strings — разбивка списка брокеров по запятой
	"strings"
	// sync — WaitGroup для ожидания завершения горутин
	"sync"
	// syscall — константы SIGINT, SIGTERM
	"syscall"

	// IBM/sarama — Kafka-клиент для Go
	"github.com/IBM/sarama"
	// kafkascram — адаптер SCRAM-SHA-512 (см. scram/scram.go)
	kafkascram "stream-processor/scram"
)

// bannedCategories — множество запрещённых категорий.
// Товары этих категорий не пропускаются в products-filtered.
// Используем map[string]struct{} для O(1) поиска (эффективнее среза).
var bannedCategories = map[string]struct{}{
	// Табачная продукция запрещена к продаже на маркетплейсе
	"tobacco": {},
	// Алкогольные напитки запрещены
	"alcohol": {},
	// Оружие запрещено
	"weapons": {},
}

// Product — структура товара из топика products-raw.
// Должна совпадать со структурой в shop-api/main.go.
type Product struct {
	// Уникальный идентификатор товара
	ID string `json:"id"`
	// Название товара
	Name string `json:"name"`
	// Категория — ключевое поле для фильтрации
	Category string `json:"category"`
	// Описание товара
	Description string `json:"description"`
	// Цена в рублях
	Price float64 `json:"price"`
	// Валюта (всегда RUB в нашем проекте)
	Currency string `json:"currency"`
	// Остаток на складе
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

// getEnv возвращает значение переменной окружения или defaultValue.
func getEnv(key, defaultValue string) string {
	// Если переменная задана и не пуста — возвращаем её значение
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// buildTLSConfig создаёт TLS-конфигурацию с нашим CA.
func buildTLSConfig(caFile string) (*tls.Config, error) {
	// Читаем CA сертификат из файла (PEM формат)
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать CA %s: %w", caFile, err)
	}

	// Создаём изолированный пул — не доверяем системным CA, только нашему
	caCertPool := x509.NewCertPool()
	// Парсим PEM и добавляем в пул доверенных CA
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("не удалось добавить CA в пул — проверьте PEM формат")
	}

	// Возвращаем TLS конфиг только с нашим CA
	return &tls.Config{RootCAs: caCertPool}, nil
}

// buildSaramaConfig создаёт базовый sarama.Config с SASL/SCRAM-SHA-512 и TLS.
func buildSaramaConfig(user, password string, tlsCfg *tls.Config) *sarama.Config {
	// Начинаем с дефолтных настроек
	cfg := sarama.NewConfig()

	// Kafka 3.7 (sarama V3_6_0_0 совместима с ней)
	cfg.Version = sarama.V3_6_0_0

	// ── SASL ──────────────────────────────────────────────────────────────────

	// Включаем SASL аутентификацию
	cfg.Net.SASL.Enable = true
	// Механизм: SCRAM-SHA-512 (задан в docker-compose.yml)
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	// Учётные данные сервиса stream-processor
	cfg.Net.SASL.User = user
	cfg.Net.SASL.Password = password
	// Handshake v1 — стандарт для Kafka
	cfg.Net.SASL.Handshake = true
	// Фабрика SCRAM-клиентов
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// ── TLS ───────────────────────────────────────────────────────────────────

	// Включаем TLS шифрование
	cfg.Net.TLS.Enable = true
	// Используем TLS конфиг с нашим CA
	cfg.Net.TLS.Config = tlsCfg

	return cfg
}

// filterHandler — обработчик сообщений consumer group.
// Реализует интерфейс sarama.ConsumerGroupHandler.
//
// Жизненный цикл вызовов sarama:
//   Setup → [ConsumeClaim вызывается для каждой партиции] → Cleanup
//
// Setup и Cleanup вызываются при каждой балансировке (rebalance).
type filterHandler struct {
	// producer — синхронный продюсер для записи разрешённых товаров
	producer sarama.SyncProducer
	// topicOut — имя выходного топика (products-filtered)
	topicOut string
	// processed — счётчик обработанных сообщений (для лога)
	processed int
	// filtered — счётчик отфильтрованных сообщений (для лога)
	filtered int
	// mu — мьютекс для защиты счётчиков (несколько горутин ConsumeClaim)
	mu sync.Mutex
}

// Setup вызывается sarama перед началом потребления партиций.
// У нас нет специальной инициализации — возвращаем nil.
func (h *filterHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup вызывается sarama после завершения потребления партиций (rebalance или shutdown).
// Выводим итоговую статистику обработки.
func (h *filterHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// Блокируем мьютекс чтобы безопасно прочитать счётчики
	h.mu.Lock()
	defer h.mu.Unlock()
	// Выводим итоговую статистику сессии
	log.Printf("Сессия завершена: обработано=%d, отфильтровано=%d, передано=%d",
		h.processed, h.filtered, h.processed-h.filtered)
	return nil
}

// ConsumeClaim — основной метод обработки сообщений.
// Вызывается sarama в отдельной горутине для каждой назначенной партиции.
//
// session — контекст текущей сессии (для ACK/commit оффсетов)
// claim  — канал сообщений из конкретной партиции
func (h *filterHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Читаем сообщения из канала партиции
	// Цикл завершается когда канал закрывается (при shutdown или rebalance)
	for msg := range claim.Messages() {
		// Десериализуем JSON в структуру Product
		var product Product
		if err := json.Unmarshal(msg.Value, &product); err != nil {
			// Логируем невалидное сообщение и пропускаем его
			log.Printf("WARN: невалидный JSON (partition=%d offset=%d): %v",
				msg.Partition, msg.Offset, err)
			// Отмечаем оффсет как обработанный даже при ошибке парсинга
			session.MarkMessage(msg, "")
			continue
		}

		// Блокируем мьютекс для обновления счётчиков
		h.mu.Lock()
		h.processed++
		h.mu.Unlock()

		// Проверяем категорию товара в множестве запрещённых
		// Поиск в map: O(1), не зависит от числа запрещённых категорий
		if _, banned := bannedCategories[product.Category]; banned {
			// Товар запрещён — не передаём в products-filtered
			h.mu.Lock()
			h.filtered++
			h.mu.Unlock()
			// Логируем для наглядности (артефакт для куратора — видно в stdout)
			log.Printf("FILTERED [%s] %s (category=%s) — запрещённая категория",
				product.ID, product.Name, product.Category)
			// Отмечаем оффсет обработанным
			session.MarkMessage(msg, "")
			continue
		}

		// Товар разрешён — отправляем в products-filtered
		// Сериализуем обратно в JSON (можно передать msg.Value напрямую, но
		// пересериализация подтверждает что мы успешно распарсили и переотправляем валидный объект)
		outValue, err := json.Marshal(product)
		if err != nil {
			log.Printf("ERROR: ошибка сериализации %s: %v", product.ID, err)
			session.MarkMessage(msg, "")
			continue
		}

		// Формируем сообщение для products-filtered
		outMsg := &sarama.ProducerMessage{
			Topic: h.topicOut,
			// Ключ сохраняем из входного сообщения — сохраняем партиционирование по ID товара
			Key:   sarama.ByteEncoder(msg.Key),
			Value: sarama.ByteEncoder(outValue),
		}

		// Публикуем в products-filtered (синхронно — ждём ACK)
		partition, offset, err := h.producer.SendMessage(outMsg)
		if err != nil {
			// Ошибка публикации — логируем, не отмечаем оффсет чтобы повторить
			log.Printf("ERROR: не удалось записать %s в %s: %v", product.ID, h.topicOut, err)
			continue
		}

		// Успешно опубликовали — логируем для наглядности
		log.Printf("PASSED  [%s] %s (category=%s) → %s partition=%d offset=%d",
			product.ID, product.Name, product.Category, h.topicOut, partition, offset)

		// Отмечаем входной оффсет как обработанный (enable.auto.commit=false)
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	// ── Конфигурация из переменных окружения ──────────────────────────────────

	// Брокеры кластера 1 — основного кластера с продуктами
	brokersStr := getEnv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
	brokers := strings.Split(brokersStr, ",")

	// Учётные данные сервиса stream-processor (создан в cluster-1/setup.sh)
	user := getEnv("KAFKA_USER", "stream-processor")
	password := getEnv("KAFKA_PASSWORD", "StreamPass1")

	// Путь к CA сертификату
	caFile := getEnv("KAFKA_TLS_CA", "../step-2-kafka/ssl/certs/ca.crt")

	// Входной топик — все товары от shop-api
	topicIn := getEnv("TOPIC_IN", "products-raw")
	// Выходной топик — только разрешённые товары
	topicOut := getEnv("TOPIC_OUT", "products-filtered")
	// Consumer group ID — должен быть уникальным для этого сервиса
	groupID := getEnv("GROUP_ID", "stream-processor")

	log.Printf("Stream Processor запускается...")
	log.Printf("  Брокеры:   %s", brokersStr)
	log.Printf("  Вход:      %s", topicIn)
	log.Printf("  Выход:     %s", topicOut)
	log.Printf("  Группа:    %s", groupID)
	log.Printf("  Запрещены: tobacco, alcohol, weapons")

	// ── TLS конфигурация ───────────────────────────────────────────────────────

	tlsCfg, err := buildTLSConfig(caFile)
	if err != nil {
		log.Fatalf("TLS ошибка: %v", err)
	}

	// ── Producer конфигурация ──────────────────────────────────────────────────

	// Конфиг для продюсера (записи в products-filtered)
	producerCfg := buildSaramaConfig(user, password, tlsCfg)

	// Ждём подтверждения от всех реплик ISR — надёжная запись
	producerCfg.Producer.RequiredAcks = sarama.WaitForAll
	// Идемпотентность — защита от дублирования при ретраях
	producerCfg.Producer.Idempotent = true
	// При Idempotent=true: MaxOpenRequests должен быть 1
	producerCfg.Net.MaxOpenRequests = 1
	// Возвращать результаты (нужно для SyncProducer)
	producerCfg.Producer.Return.Successes = true
	producerCfg.Producer.Return.Errors = true

	// Создаём синхронный продюсер
	producer, err := sarama.NewSyncProducer(brokers, producerCfg)
	if err != nil {
		log.Fatalf("Не удалось создать продюсер: %v\nПодсказка: проверьте что cluster-1 запущен и /etc/hosts настроен", err)
	}
	defer producer.Close()

	// ── Consumer Group конфигурация ────────────────────────────────────────────

	// Конфиг для consumer group
	consumerCfg := buildSaramaConfig(user, password, tlsCfg)

	// OffsetOldest — читаем с самого начала (чтобы не пропустить товары)
	// В продакшне использовали бы OffsetNewest или сохранённый оффсет
	consumerCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	// AutoCommit.Enable = false — подтверждаем оффсеты вручную через MarkMessage
	// Это защищает от потери сообщений: оффсет сдвигается только после успешной обработки
	consumerCfg.Consumer.Offsets.AutoCommit.Enable = false

	// Создаём consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, consumerCfg)
	if err != nil {
		log.Fatalf("Не удалось создать consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// ── Graceful Shutdown ──────────────────────────────────────────────────────

	// Канал для получения сигналов завершения
	sigChan := make(chan os.Signal, 1)
	// Подписываемся на SIGINT (Ctrl+C) и SIGTERM (kill)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// context.WithCancel — контекст с возможностью отмены (для consumer group)
	ctx, cancel := context.WithCancel(context.Background())

	// Горутина: при получении сигнала отменяем контекст → consumer group завершится
	go func() {
		sig := <-sigChan
		log.Printf("Получен сигнал %v, завершаем работу...", sig)
		// cancel() закрывает ctx.Done() канал → Consume() вернёт управление
		cancel()
	}()

	// ── Обработчик сообщений ───────────────────────────────────────────────────

	// Создаём обработчик с ссылкой на продюсер и выходной топик
	handler := &filterHandler{
		producer: producer,
		topicOut: topicOut,
	}

	// ── Основной цикл ─────────────────────────────────────────────────────────

	log.Printf("Начинаю обработку топика %s...", topicIn)

	// WaitGroup для ожидания завершения горутины consumer loop
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// Consume — блокирующий вызов, возвращается при ошибке или отмене контекста
		// topics — список топиков для подписки
		topics := []string{topicIn}

		for {
			// Consume инициирует сессию consumer group
			// При rebalance (новый брокер, изменение партиций) — вызывается снова
			if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
				// context.Canceled — нормальное завершение при отмене контекста
				if ctx.Err() != nil {
					return // Выходим из цикла (shutdown)
				}
				// Любая другая ошибка — логируем и пробуем переподключиться
				log.Printf("Ошибка consumer group: %v", err)
			}

			// Проверяем отменён ли контекст перед следующей итерацией
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Ждём завершения consumer loop
	wg.Wait()
	log.Println("Stream Processor остановлен.")
}
