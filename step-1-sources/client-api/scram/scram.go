// Пакет scram — адаптер SCRAM-SHA-512 для библиотеки IBM/sarama.
//
// IBM/sarama не включает реализацию SCRAM «из коробки»: в Go нужно:
//   1. Подключить внешний пакет github.com/xdg-go/scram
//   2. Написать тип, реализующий интерфейс sarama.SCRAMClient
//   3. Передать генератор этого типа через cfg.Net.SASL.SCRAMClientGeneratorFunc
//
// Без этого пакета sarama падает с ошибкой "SCRAM mechanism not found".
// Этот паттерн стандартен для Go + Kafka: см. также PW7/client/scram/scram.go.
package scram

import (
	// SHA-256 — для SCRAM-SHA-256 (не используется здесь, но нужен для полноты)
	"crypto/sha256"
	// SHA-512 — для SCRAM-SHA-512, обязательный механизм в нашем кластере
	"crypto/sha512"
	// hash.Hash — общий интерфейс для всех хеш-функций в Go
	"hash"

	// sarama — Kafka-клиент, определяет интерфейс SCRAMClient
	"github.com/IBM/sarama"
	// scram — реализация протокола SCRAM (RFC 5802) на чистом Go
	"github.com/xdg-go/scram"
)

// SHA256 — генератор хеш-функции SHA-256.
// Тип scram.HashGeneratorFcn — это func() hash.Hash.
var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// SHA512 — генератор хеш-функции SHA-512.
// Используется в нашем кластере: KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=SCRAM-SHA-512.
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

// XDGSCRAMClient — адаптер между sarama.SCRAMClient и xdg-go/scram.
//
// Жизненный цикл: sarama вызывает Begin → несколько раз Step → Done.
// Begin — инициализация перед соединением.
// Step  — один шаг обмена challenge/response.
// Done  — проверка что аутентификация завершена.
type XDGSCRAMClient struct {
	// *scram.Client — хранит учётные данные (логин/пароль) и алгоритм хеширования
	*scram.Client
	// *scram.ClientConversation — управляет состоянием текущего SCRAM-обмена
	*scram.ClientConversation
	// HashGeneratorFcn — функция создания хеш-объекта (SHA256 или SHA512)
	scram.HashGeneratorFcn
}

// Begin — инициализация SCRAM клиента перед SASL-рукопожатием.
// Вызывается sarama один раз на каждое новое TCP-соединение с брокером.
//
// userName, password — учётные данные (из KAFKA_USER / KAFKA_PASSWORD).
// authzID — authorization ID, обычно "" (пустой = совпадает с userName).
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	var err error
	// NewClient создаёт SCRAM-клиент с нужным хеш-алгоритмом и учётными данными
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		// Возвращаем ошибку в sarama — соединение не установится
		return err
	}
	// NewConversation создаёт объект для отслеживания состояния обмена challenge/response
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step — один шаг SCRAM-обмена.
// Вызывается несколько раз: первый раз с "" (клиент начинает handshake),
// затем с challenge от брокера, пока Done() не вернёт true.
//
// Возвращает строку-ответ для отправки брокеру.
func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	// ClientConversation.Step обрабатывает challenge и возвращает ответ
	return x.ClientConversation.Step(challenge)
}

// Done — возвращает true, когда SCRAM-аутентификация успешно завершена.
// sarama вызывает после последнего Step чтобы подтвердить успех.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// Статическая проверка совместимости: если убрать любой из методов выше —
// компилятор сообщит об ошибке здесь, а не в рантайме при подключении.
var _ sarama.SCRAMClient = &XDGSCRAMClient{}
