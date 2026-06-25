# Артефакты финального проекта — Аналитическая платформа маркетплейса

Яндекс Практикум · Apache Kafka  
Дата запуска: 2026-06-24

## Сводка проверки

| Компонент | Что проверено | Результат |
|---|---|---|
| Kafka cluster-1 | 3 брокера (KRaft, SASL/TLS, JMX) | ✅ healthy |
| Kafka cluster-2 | 1 брокер kafka-analytics | ✅ healthy |
| MirrorMaker 2 | Репликация cluster1.* в cluster-2 | ✅ работает |
| SHOP API (Go) | Публикация товаров в products-raw | ✅ 3640+ сообщений |
| Stream Processor (Go) | Фильтрация tobacco/alcohol/weapons | ✅ работает |
| Kafka Connect | FileStreamSink → products-filtered.jsonl | ✅ 2404 строки |
| PySpark Structured Streaming | Топик recommendations создан, Spark запускался | ⚠️ OOM на macOS |
| Prometheus | Scrape kafka-1:9101, kafka-2:9102, kafka-3:9103 | ✅ все up |
| Grafana | Дашборд брокеров и топиков | ✅ доступна |
| Alertmanager | Маршрутизация алертов → Telegram | ✅ работает |

## Файлы артефактов

- `01_cluster1_topics.txt` — топики кластера 1: products-raw, products-filtered, client-events (RF=3, 3 партиции)
- `02_cluster2_topics.txt` — топики кластера 2: cluster1.* (MirrorMaker), recommendations
- `03_stream_processor_log.txt` — лог фильтрации: PASSED (категории разрешены) / FILTERED (tobacco/alcohol/weapons)
- `04_products_filtered_sample.txt` — первые 5 строк + итого 2404 строки в products-filtered.jsonl
- `05_prometheus_targets.txt` — kafka-1/2/3 up; kafka-analytics down (JMX не настроен на cluster-2)
- `06_recommendations_sample.txt` — топик существует (см. 02), снимок не захвачен из-за Spark OOM
- `07_consumer_groups.txt` — consumer groups: stream-processor (products-raw), connect (products-filtered, lag=2-3)
- `08_tls_cert_kafka2.txt` — TLS сертификат kafka-2 с SAN: DNS:kafka-2, IP:127.0.0.1

## Ключевые метрики (на момент проверки)

**products-raw** (кластер 1):
- partition=0: ~580 сообщений
- partition=1: ~2250 сообщений
- partition=2: ~810 сообщений
- Итого: ~3640 сообщений от shop-api

**products-filtered** (кластер 1):
- Kafka Connect lag: 2-3 сообщения (в реальном времени)
- Файл на диске: 2404 строки (JSONL)

**consumer groups**:
- `stream-processor` — читает products-raw, 3 партиции
- `connect-filesink-group` — читает products-filtered, lag ~2-3

## Ключевые технические решения

1. **KRaft без StandardAuthorizer** — в Kafka 3.7 combined mode возникает дедлок при старте
   с StandardAuthorizer. Отключён, SASL-аутентификация сохранена.

2. **TLS с SAN** — Go 1.15+ отклоняет CN-only сертификаты. Добавлен `subjectAltName=DNS:kafka-N`
   через `-extfile` в openssl.

3. **KAFKA_OPTS="" в healthcheck** — JMX javaagent наследуется всеми Java-процессами
   в контейнере, включая `kafka-broker-api-versions.sh`. Обнуление до вызова healthcheck.

4. **plugin.path=/opt/bitnami/kafka/libs** — FileStreamSinkConnector не включён
   в classpath connect-standalone.sh в bitnami/kafka, нужно указывать явно.

5. **Go-приложения в Docker** — с хоста kafka-2/3 недоступны на нужных портах
   (все резолвятся в 127.0.0.1:9092 = kafka-1). Решение: запуск в kafka-cluster-net.

## Ссылки

- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Alertmanager: http://localhost:9188
