// go.mod — описание Go-модуля SHOP API и его зависимостей.
//
// Модуль: shop-api (локальный путь, не требует репозитория)
//
// Основные зависимости:
//   - IBM/sarama: Go-клиент для Apache Kafka (поддерживает SASL/SCRAM-SHA-512, TLS)
//   - xdg-go/scram: реализация протокола SCRAM-SHA-512 (требуется sarama)
//
// После клонирования:
//   cd step-1-sources/shop-api && go mod tidy && go run .
module shop-api

go 1.21

require (
	// IBM/sarama — полнофункциональный Kafka клиент для Go
	// v1.43.3 — последняя стабильная версия на момент написания (совместима с Kafka 3.7)
	github.com/IBM/sarama v1.43.3
	// xdg-go/scram — реализация SCRAM (RFC 5802) на чистом Go
	// Требуется для sarama.SCRAMClientGeneratorFunc в scram/scram.go
	github.com/xdg-go/scram v1.1.2
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/text v0.17.0 // indirect
)
