// main.go — SHOP API: читает каталог товаров и публикует их в Kafka.
//
// SHOP API симулирует поток товаров от магазинов-партнёров маркетплейса.
// Каждый товар из data/products.json отправляется в топик products-raw кластера 1.
// Stream Processor (Шаг 4) затем отфильтрует запрещённые товары по категории.
//
// Режим работы:
//   products.json → сериализация в JSON → Kafka topic products-raw
//   После последнего товара — начинает цикл заново (симуляция непрерывного потока).
//   Остановка: Ctrl+C (SIGINT) или SIGTERM.
//
// Конфигурация через переменные окружения (или дефолты для локального запуска):
//   KAFKA_BROKERS     — bootstrap servers, default: kafka-1:9092,kafka-2:9092,kafka-3:9092
//   KAFKA_USER        — SASL пользователь, default: shop-api
//   KAFKA_PASSWORD    — SASL пароль, default: ShopApiPass1
//   KAFKA_TLS_CA      — путь к CA сертификату (PEM), default: ../ssl/certs/ca.crt
//   KAFKA_TOPIC       — целевой топик, default: products-raw
//   PRODUCTS_FILE     — путь к JSON каталогу, default: data/products.json
//   SEND_INTERVAL_MS  — пауза между товарами в мс, default: 1000
package main

import (
	// crypto/tls — создание TLS-конфига для шифрования трафика до брокера
	"crypto/tls"
	// crypto/x509 — работа с X.509 сертификатами: добавляем CA в пул доверенных
	"crypto/x509"
	// encoding/json — сериализация структур Go в JSON (тело сообщения Kafka)
	"encoding/json"
	// fmt — форматированный вывод в строках
	"fmt"
	// log — логирование с временными метками (log.Printf, log.Fatalf)
	"log"
	// os — переменные окружения и чтение файлов (os.Getenv, os.ReadFile)
	"os"
	// os/signal — перехват системных сигналов (SIGINT, SIGTERM)
	"os/signal"
	// strconv — конвертация строк в числа (SEND_INTERVAL_MS → int)
	"strconv"
	// strings — разбивка строки по разделителю (KAFKA_BROKERS → []string)
	"strings"
	// syscall — константы системных сигналов (syscall.SIGINT, syscall.SIGTERM)
	"syscall"
	// time — работа со временем: паузы, Duration
	"time"

	// IBM/sarama — Go-клиент для Apache Kafka (поддерживает SASL/SCRAM-SHA-512, TLS)
	"github.com/IBM/sarama"
	// kafkascram — наш адаптер SCRAM для sarama (см. scram/scram.go)
	kafkascram "shop-api/scram"
)

// Product — структура товара.
// Теги json определяют имена полей при Marshal/Unmarshal.
// Порядок полей совпадает с data/products.json для наглядности.
type Product struct {
	// Уникальный идентификатор товара, используется как Kafka message key
	ID string `json:"id"`
	// Название товара — основная информация для потребителей
	Name string `json:"name"`
	// Категория — по ней Stream Processor (Шаг 4) определяет запрещённые товары
	Category string `json:"category"`
	// Описание товара
	Description string `json:"description"`
	// Цена в валюте Currency
	Price float64 `json:"price"`
	// Валюта (RUB для всех товаров в нашем каталоге)
	Currency string `json:"currency"`
	// Остаток на складе
	Quantity int `json:"quantity"`
	// ID продавца — для аналитики по магазинам
	SellerID string `json:"seller_id"`
	// Бренд — для поиска и фильтрации
	Brand string `json:"brand"`
	// Теги — для Elasticsearch-поиска в Шаге 5
	Tags []string `json:"tags"`
	// Дата добавления в каталог (RFC3339)
	CreatedAt string `json:"created_at"`
}

// getEnv возвращает значение переменной окружения или defaultValue.
// Используется для конфигурации: позволяет запускать как локально (дефолты),
// так и в Docker (переменные из docker-compose).
func getEnv(key, defaultValue string) string {
	// os.Getenv возвращает "" если переменная не задана
	if v := os.Getenv(key); v != "" {
		return v
	}
	// Дефолт подходит для запуска с хоста при /etc/hosts kafka-1/2/3 → 127.0.0.1
	return defaultValue
}

func main() {
	// ─── Конфигурация ─────────────────────────────────────────────────────────

	// Bootstrap серверы: Kafka сообщает полный список брокеров при первом подключении
	brokersStr := getEnv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
	// strings.Split → срез строк ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
	brokers := strings.Split(brokersStr, ",")

	// shop-api — пользователь созданный в cluster-1/docker-compose.yml (KAFKA_CLIENT_USERS)
	user := getEnv("KAFKA_USER", "shop-api")
	// Пароль из KAFKA_CLIENT_PASSWORDS в docker-compose.yml
	password := getEnv("KAFKA_PASSWORD", "ShopApiPass1")

	// Путь к CA сертификату (создан ssl/generate-certs.sh)
	// Из директории step-1-sources/shop-api/ путь ../ssl/certs/ca.crt ведёт к step-2-kafka/ssl/
	caFile := getEnv("KAFKA_TLS_CA", "../ssl/certs/ca.crt")

	// Топик products-raw создан в cluster-1/setup.sh (3 партиции, RF=3, retention 7 дней)
	topic := getEnv("KAFKA_TOPIC", "products-raw")

	// Файл с каталогом товаров (относительно рабочей директории)
	productsFile := getEnv("PRODUCTS_FILE", "data/products.json")

	// Интервал между публикациями в миллисекундах
	intervalStr := getEnv("SEND_INTERVAL_MS", "1000")
	// strconv.Atoi конвертирует строку "1000" в int 1000
	intervalMs, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Fatalf("SEND_INTERVAL_MS должен быть числом, получено %q: %v", intervalStr, err)
	}
	// time.Duration(1000) * time.Millisecond = 1 секунда
	interval := time.Duration(intervalMs) * time.Millisecond

	log.Printf("SHOP API запущен")
	log.Printf("  brokers:  %s", brokersStr)
	log.Printf("  topic:    %s", topic)
	log.Printf("  user:     %s", user)
	log.Printf("  interval: %v", interval)

	// ─── Загрузка каталога товаров ─────────────────────────────────────────────

	// os.ReadFile читает весь файл в память — приемлемо для небольшого каталога (~20 товаров)
	data, err := os.ReadFile(productsFile)
	if err != nil {
		log.Fatalf("Не удалось прочитать каталог товаров %s: %v", productsFile, err)
	}

	// json.Unmarshal декодирует JSON-массив в срез структур Product
	var products []Product
	if err := json.Unmarshal(data, &products); err != nil {
		log.Fatalf("Не удалось разобрать JSON %s: %v", productsFile, err)
	}
	log.Printf("Загружено товаров: %d (из файла %s)", len(products), productsFile)

	// ─── TLS конфигурация ─────────────────────────────────────────────────────

	// Читаем CA сертификат — им подписаны сертификаты всех брокеров (kafka-1/2/3)
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Не удалось прочитать CA сертификат %s: %v\nПодсказка: запустите сначала bash step-2-kafka/ssl/generate-certs.sh", caFile, err)
	}

	// x509.NewCertPool — создаём изолированный пул доверенных CA
	// Не добавляем системные CA — доверяем только нашему внутреннему CA
	caCertPool := x509.NewCertPool()
	// AppendCertsFromPEM парсит PEM-блоки и добавляет сертификаты в пул
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		log.Fatal("Не удалось добавить CA в пул — проверьте что файл в PEM формате")
	}

	// tls.Config управляет поведением TLS-соединения
	tlsCfg := &tls.Config{
		// RootCAs — пул CA для проверки сертификата сервера (брокера)
		// Соединение будет отклонено если сертификат брокера не подписан нашим CA
		RootCAs: caCertPool,
	}

	// ─── Kafka (Sarama) конфигурация ──────────────────────────────────────────

	// sarama.NewConfig() — конфиг с разумными дефолтами от IBM
	cfg := sarama.NewConfig()

	// Версия протокола Kafka: брокер 3.7, sarama V3_6_0_0 совместима
	// (sarama не имеет константы V3_7, V3_6 работает с 3.7 брокерами)
	cfg.Version = sarama.V3_6_0_0

	// ── SASL/SCRAM-SHA-512 ────────────────────────────────────────────────────

	// Enable — активируем SASL аутентификацию
	cfg.Net.SASL.Enable = true
	// SASLTypeSCRAMSHA512 — механизм аутентификации (тот что задан в docker-compose)
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	// User — имя пользователя (создан bitnami при старте контейнера)
	cfg.Net.SASL.User = user
	// Password — пароль из KAFKA_CLIENT_PASSWORDS
	cfg.Net.SASL.Password = password
	// Handshake=true — используем протокол SASL Handshake v1 (стандарт для Kafka)
	cfg.Net.SASL.Handshake = true

	// SCRAMClientGeneratorFunc — фабрика SCRAM-клиентов.
	// sarama создаёт новое TCP-соединение → вызывает эту функцию → получает
	// свежий XDGSCRAMClient → вызывает Begin, Step, Done на нём.
	// Функция должна возвращать НОВЫЙ экземпляр на каждый вызов (не синглтон).
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		// SHA512 — та же функция что в scram.go, генерирует sha512.New()
		return &kafkascram.XDGSCRAMClient{HashGeneratorFcn: kafkascram.SHA512}
	}

	// ── TLS ───────────────────────────────────────────────────────────────────

	// Enable=true — включаем TLS, без него SASL credentials летят в открытом виде
	cfg.Net.TLS.Enable = true
	// Config — наш TLS-конфиг с CA пулом
	cfg.Net.TLS.Config = tlsCfg

	// ── Producer настройки ────────────────────────────────────────────────────

	// WaitForAll = acks=-1: ждём подтверждения от ALL ISR реплик
	// Это самый надёжный режим: сообщение не потеряется даже при падении лидера
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Идемпотентный producer: брокер назначает sequence number, отклоняет дубли.
	// Защищает от дублирования при повторной отправке после сетевого сбоя.
	cfg.Producer.Idempotent = true

	// MaxOpenRequests=1 обязательно при Idempotent=true.
	// Иначе параллельные запросы могут нарушить порядок sequence number'ов.
	cfg.Net.MaxOpenRequests = 1

	// Return.Successes=true — SyncProducer требует этого для работы канала успехов
	cfg.Producer.Return.Successes = true
	// Return.Errors=true — аналогично для канала ошибок
	cfg.Producer.Return.Errors = true

	// ─── Создание Kafka producer ──────────────────────────────────────────────

	// SyncProducer — синхронный: блокирует до получения ACK от брокера.
	// Удобен для учебного проекта: видно partition и offset каждого сообщения.
	// Для high-throughput лучше AsyncProducer (не блокирует, higher throughput).
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Не удалось подключиться к Kafka: %v\nПодсказка: проверьте что кластер запущен и /etc/hosts содержит kafka-1/2/3", err)
	}
	// defer гарантирует flush и закрытие при завершении программы (даже при panic)
	defer producer.Close()

	log.Println("Подключение к Kafka установлено!")

	// ─── Graceful shutdown ────────────────────────────────────────────────────

	// Буферизованный канал на 1 сигнал — не пропустим сигнал если goroutine не готова
	sigChan := make(chan os.Signal, 1)
	// Notify регистрирует sigChan для получения SIGINT (Ctrl+C) и SIGTERM (docker stop)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// done — сигнал главному циклу о необходимости завершения
	// Используем канал (не bool) чтобы проверка была non-blocking через select/default
	done := make(chan struct{})
	go func() {
		sig := <-sigChan // Ожидаем сигнал блокирующим receive
		log.Printf("Получен сигнал %v, завершаем после текущего товара...", sig)
		close(done) // Закрытие небуферизованного канала разблокирует все читателей
	}()

	// ─── Основной цикл публикации ─────────────────────────────────────────────

	log.Printf("Начинаю публикацию в топик %q (интервал %v)...", topic, interval)

	// sentTotal — суммарный счётчик отправленных сообщений за всё время работы
	sentTotal := 0
	// cycle — номер прохода по каталогу (для логов)
	cycle := 0

	for {
		cycle++
		log.Printf("── Цикл %d: отправляю %d товаров ──", cycle, len(products))

		// Итерируемся по загруженному каталогу
		for _, product := range products {

			// Проверяем сигнал завершения ПЕРЕД отправкой каждого товара.
			// select с default — non-blocking: не блокируется если нет сигнала.
			select {
			case <-done:
				// Получили сигнал — завершаем с отчётом
				log.Printf("Завершение: отправлено всего %d сообщений (%d цикл(ов))", sentTotal, cycle)
				return
			default:
				// Нет сигнала — продолжаем обработку
			}

			// Сериализуем структуру Product в JSON для тела сообщения Kafka.
			// Каждое сообщение в products-raw = один товар в JSON-формате.
			value, err := json.Marshal(product)
			if err != nil {
				// Ошибка сериализации — маловероятна для наших данных, но обрабатываем
				log.Printf("[ПРОПУСК] Ошибка сериализации %s: %v", product.ID, err)
				continue
			}

			// ProducerMessage — одно сообщение Kafka
			msg := &sarama.ProducerMessage{
				Topic: topic,
				// Ключ = ID товара: Kafka гарантирует что сообщения с одним ключом
				// всегда попадают в одну партицию (порядок сохраняется по ключу).
				// StringEncoder конвертирует string в []byte для sarama.
				Key: sarama.StringEncoder(product.ID),
				// Тело сообщения = JSON товара
				// ByteEncoder конвертирует []byte — уже сериализованный JSON.
				Value: sarama.ByteEncoder(value),
			}

			// SendMessage — синхронная отправка, возвращает partition и offset.
			// Блокируется до получения ack от всех ISR (RequiredAcks=WaitForAll).
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("[ОШИБКА] Не удалось отправить %s: %v", product.ID, err)
				continue
			}

			sentTotal++
			// Подробный лог каждого отправленного сообщения
			log.Printf("[%4d] → %s | %s | %.2f ₽ | partition=%d offset=%d",
				sentTotal,
				product.ID,
				fmt.Sprintf("%-40s", product.Name), // выравнивание для читаемости
				product.Price,
				partition,
				offset,
			)

			// Пауза между отправками — симулируем реальный темп поступления товаров
			time.Sleep(interval)
		}

		log.Printf("Цикл %d завершён, всего отправлено: %d", cycle, sentTotal)
	}
}
