// go.mod — модуль Stream Processor (Goka-based).
//
// Потребляет из products-raw, фильтрует по категории через Goka,
// публикует разрешённые товары в products-filtered.
//
// После клонирования:
//   cd step-4-stream-processor
//   go mod tidy    # скачать зависимости
//   go run . list  # проверить список запрещённых категорий
//   go run .       # запустить процессор
module stream-processor

go 1.25.0

require (
	// IBM/sarama — Kafka-клиент для Go (используется внутри Goka)
	github.com/IBM/sarama v1.46.3
	// lovoo/goka — фреймворк потоковой обработки поверх sarama
	// Предоставляет DeclarativeGroup, Processor, Context.Emit
	github.com/lovoo/goka v1.1.16
	// xdg-go/scram — SCRAM-SHA-512 для SASL аутентификации
	github.com/xdg-go/scram v1.1.2
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	go.uber.org/mock v0.6.0 // indirect
	golang.org/x/crypto v0.47.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.33.0 // indirect
)
