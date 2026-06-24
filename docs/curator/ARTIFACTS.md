# Артефакты финального проекта — Аналитическая платформа маркетплейса

Яндекс Практикум · Apache Kafka  
Дата запуска: 2026-06-24

## Сводка проверки

| Компонент | Что проверено | Результат |
|---|---|---|
| Kafka cluster-1 | 3 брокера (KRaft, SASL/TLS, JMX) | ✅ healthy |
| Kafka cluster-2 | 1 брокер kafka-analytics | ✅ healthy |
| MirrorMaker 2 | Репликация cluster1.* в cluster-2 | ✅ работает |
| SHOP API (Go) | Публикация товаров в products-raw | ✅ 3000+ сообщений |
| Stream Processor (Go) | Фильтрация tobacco/alcohol/weapons | ✅ работает |
| Kafka Connect | FileStreamSink → products-filtered.jsonl | ✅ 1900+ строк |
| PySpark Structured Streaming | Рекомендации из client-events | ✅ данные в топике |
| Prometheus | Scrape kafka-1:9101, kafka-2:9102, kafka-3:9103 | ✅ все up |
| Grafana | Дашборд брокеров и топиков | ✅ доступна |
| Alertmanager | Маршрутизация алертов → Telegram | ✅ работает |

## Файлы артефактов

- `01_cluster1_topics.txt` — список и конфигурация топиков кластера 1
- `02_cluster2_topics.txt` — список и конфигурация топиков кластера 2
- `03_stream_processor_log.txt` — лог фильтрации (PASSED/FILTERED)
- `04_products_filtered_sample.txt` — первые строки products-filtered.jsonl
- `05_prometheus_targets.txt` — статус Prometheus targets
- `06_recommendations_sample.txt` — сообщения из топика recommendations
- `07_consumer_groups.txt` — consumer groups и их lag
- `08_tls_cert_kafka1.txt` — TLS сертификаты с SAN

## Ключевые метрики (на момент проверки)

**products-raw** (кластер 1):
- partition=0: ~580 сообщений
- partition=1: ~2250 сообщений
- partition=2: ~810 сообщений
- Итого: ~3640 сообщений от shop-api

**products-filtered** (кластер 1):
- Kafka Connect lag: 2-3 сообщения (в реальном времени)
- Файл: ~1900 строк

**consumer groups**:
- `stream-processor` — читает products-raw
- `connect-filesink-group` — читает products-filtered, lag ~2

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
