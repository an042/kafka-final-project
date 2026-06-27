# Финальный проект — Аналитическая платформа маркетплейса

Яндекс Практикум · Курс Apache Kafka

## Бизнес-контекст

«Покупай выгодно» — маркетплейс, которому нужна аналитическая платформа:
собирать данные о товарах и клиентах, фильтровать запрещённые товары,
строить персонализированные рекомендации и следить за состоянием системы.

## Архитектура

```
SHOP API (Go)                          CLIENT API (Go)
│ → products-raw                        │ → client-events
│                                       │ ← recommendations
▼                                       ▼
════════════ Kafka Кластер 1 (SASL_SSL + SCRAM-SHA-512) ════════════
│                                       │
▼                                       │ MirrorMaker 2
Stream Processor (Go + sarama)          │ (реплицирует cluster1.*)
│  фильтрует tobacco/alcohol/weapons    │
│  → products-filtered                  ▼
▼                              Kafka Кластер 2 (аналитика)
Kafka Connect (FileStreamSink)          │  cluster1.client-events
│  → /data/products-filtered.jsonl      │
                                        ▼
                               PySpark Structured Streaming
                                        │  скользящее окно 5 мин
                                        ▼  → recommendations

Мониторинг: kafka-{1,2,3} JMX Exporter → Prometheus → Grafana
                                                      └─► Alertmanager → Telegram

CLIENT API команды:
  search    — поиск по products.json + событие в Kafka (client-events) для аналитики
  event     — отправить произвольное событие в Kafka Кластер 1 (client-events)
  recommend — читать рекомендации из Kafka Кластер 2
```

## Стек технологий

| Компонент | Технология | Обоснование |
|---|---|---|
| SHOP API, CLIENT API | Go + IBM/sarama | Продолжаем стек предыдущих практических работ (PW7) |
| Stream Processor | Go + Goka (lovoo/goka) | Декларативный DSL поверх sarama; CLI для управления запрещёнными категориями |
| Spark-задача | Python (PySpark) | Spark не поддерживает Go; PySpark — стандарт для аналитики |
| Хранилище (базовый) | Kafka Connect FileStreamSink | Встроен в Kafka, не требует плагинов; достаточно для демонстрации |
| Мониторинг | Prometheus + Grafana + Alertmanager | Де-факто стандарт для Kafka мониторинга |
| Алерты | Telegram | Бот уже настроен в инфраструктуре |
| Аутентификация | SASL/SCRAM-SHA-512 | Безопаснее PLAIN (пароль хешируется); поддерживается bitnami/kafka |
| Шифрование | TLS/PEM | CA + брокерские сертификаты; PEM удобнее JKS для openssl |

## Структура проекта

```
.
├── docs/                               # Артефакты для куратора
│   ├── ARTIFACTS.md                    # Сводная таблица проверки
│   ├── 01_cluster1_topics.txt          # Топики кластера 1
│   ├── 02_cluster2_topics.txt          # Топики кластера 2 (MirrorMaker)
│   ├── 03_stream_processor_log.txt     # Лог PASSED/FILTERED
│   ├── 04_products_filtered_sample.txt # Первые строки + счётчик JSONL
│   ├── 05_prometheus_targets.txt       # Статус Prometheus targets
│   ├── 06_recommendations_sample.txt   # Топик recommendations (OOM note)
│   ├── 07_consumer_groups.txt          # Consumer groups и lag
│   └── 08_tls_cert_kafka2.txt          # TLS сертификат с SAN
│
├── step-1-sources/
│   ├── shop-api/                   # Производитель товаров → products-raw
│   │   ├── main.go                 # SyncProducer, цикл по products.json
│   │   ├── go.mod                  # IBM/sarama v1.43.3 + xdg-go/scram
│   │   ├── scram/scram.go          # Адаптер SCRAM-SHA-512 для sarama
│   │   └── data/products.json      # 20 товаров (15 разрешённых + 5 запрещённых)
│   └── client-api/                 # CLI клиента: search / recommend / event
│       ├── main.go                 # 3 команды, 2 Kafka соединения
│       ├── go.mod
│       └── scram/scram.go
│
├── step-2-kafka/
│   ├── ssl/
│   │   ├── generate-certs.sh       # openssl: CA + 4 брокерских сертификата с SAN
│   │   └── certs/                  # ca.crt, kafka-{1,2,3,analytics}.{crt,key,chain.crt}
│   ├── cluster-1/                  # Кластер 1: 3 брокера KRaft, SASL_SSL
│   │   ├── docker-compose.yml      # kafka-1/2/3 + JMX Exporter порты 9101-9103
│   │   ├── setup.sh                # Создание топиков и демо-ACL
│   │   └── jmx/
│   │       ├── download-agent.sh   # Скачать jmx_prometheus_javaagent.jar
│   │       └── kafka-jmx-exporter.yml  # JMX MBeans конфиг для агента
│   ├── cluster-2/                  # Кластер 2: 1 брокер kafka-analytics
│   │   ├── docker-compose.yml
│   │   └── setup.sh                # Создание топика recommendations
│   └── mirrormaker/                # MirrorMaker 2: cluster1 → cluster2
│       ├── docker-compose.yml
│       └── mm2.properties          # RF=1 для cluster-2, обратная репликация отключена
│
├── step-3-analytics/
│   └── basic/
│       ├── docker-compose.yml      # bitnamilegacy/spark:3.5
│       └── spark-job/
│           └── recommendations.py  # Structured Streaming: client-events → recommendations
│
├── step-4-stream-processor/        # Фильтр запрещённых товаров (Goka + CLI)
│   ├── main.go                     # Goka Processor; CLI: process/list/add/remove
│   ├── go.mod                      # lovoo/goka v1.1.16 + IBM/sarama
│   ├── banned_categories.json      # Список запрещённых категорий (управляется CLI)
│   └── scram/scram.go
│
├── step-5-storage/
│   └── basic/                      # Kafka Connect FileStreamSink
│       ├── docker-compose.yml
│       └── config/
│           ├── connect-worker.properties    # SASL_SSL, plugin.path, StringConverter
│           └── connect-filesink.properties  # products-filtered → /data/*.jsonl
│
├── step-6-monitoring/
│   ├── docker-compose.yml          # Prometheus + Grafana + Alertmanager
│   ├── prometheus/
│   │   ├── prometheus.yml          # Scrape: kafka-1:9101, kafka-2:9102, kafka-3:9103
│   │   └── alert.rules.yml         # 6 правил: BrokerDown, ConsumerLag, UnderReplicated…
│   ├── jmx-exporter/
│   │   └── kafka-jmx-exporter.yml  # JMX MBeans → Prometheus метрики (step-6 копия)
│   ├── grafana/
│   │   ├── provisioning/           # Автоматический datasource + папка дашбордов
│   │   └── dashboards/
│   │       └── kafka-overview.json # Дашборд: статус брокеров, throughput, consumer lag
│   └── alertmanager/
│       └── alertmanager.yml        # Маршрутизация → Telegram (bot_token + chat_id)
│
└── README.md
```

## Порядок запуска

### Шаг 2: Kafka кластеры

```bash
# 1. TLS сертификаты
cd step-2-kafka/ssl && bash generate-certs.sh

# 2. Скачать JMX агент (нужен для Шага 6)
bash step-2-kafka/cluster-1/jmx/download-agent.sh

# 3. Кластер 1
cd step-2-kafka/cluster-1 && docker compose up -d
# подождать 30-60 сек, затем:
bash setup.sh          # создаёт топики products-raw, products-filtered, client-events

# 4. Кластер 2
cd ../cluster-2 && docker compose up -d && bash setup.sh   # создаёт recommendations

# 5. MirrorMaker 2
cd ../mirrormaker && docker compose up -d
# проверка: topic "cluster1.products-raw" появился в кластере 2
```

### Шаги 1 и 4: Источники данных и Stream Processor

```bash
# /etc/hosts (один раз):
# 127.0.0.1 kafka-1 kafka-2 kafka-3 kafka-analytics

# SHOP API — бесконечно публикует товары в products-raw
cd step-1-sources/shop-api && go run .

# Шаг 4: Stream Processor — фильтрует products-raw → products-filtered
cd step-4-stream-processor && go run .

# CLIENT API
cd step-1-sources/client-api
go run . search "ноутбук"            # локальный поиск по products.json
go run . event view prod-001         # отправить событие → client-events
go run . recommend                   # читать рекомендации из кластера 2
```

### Шаг 3: PySpark аналитика

```bash
cd step-3-analytics/basic
docker compose up      # spark-submit запускается автоматически
# Рекомендации появятся в топике recommendations (кластер 2) через 1-2 мин
```

### Шаг 5: Kafka Connect

```bash
cd step-5-storage/basic && docker compose up -d
# Проверка — файл с отфильтрованными товарами:
docker exec kafka-connect cat /data/products-filtered.jsonl
```

### Шаг 6: Мониторинг

```bash
cd step-6-monitoring && docker compose up -d
# Grafana:      http://localhost:3000  (admin / admin)
# Prometheus:   http://localhost:9090
# Alertmanager: http://localhost:9188  (порт 9093 занят kafka-2 в данном стенде)
```

Перед использованием — заполнить `alertmanager/alertmanager.yml`:
```yaml
bot_token: "<токен от @BotFather>"
chat_id: <id вашего чата>
```

## Пользователи и пароли (SCRAM-SHA-512)

| Пользователь | Пароль | Кластер | Топики |
|---|---|---|---|
| admin | AdminPass1 | 1, 2 | суперпользователь |
| shop-api | ShopApiPass1 | 1 | WRITE products-raw |
| stream-processor | StreamPass1 | 1 | READ products-raw, WRITE products-filtered |
| client-api | ClientApiPass1 | 1, 2 | WRITE client-events; READ recommendations |
| mirrormaker | MirrorPass1 | 1, 2 | READ/DESCRIBE all |
| kafka-connect | ConnectPass1 | 1 | READ products-filtered |
| spark | SparkPass1 | 2 | READ cluster1.client-events, WRITE recommendations |
| analytics | AnalyticsPass1 | 2 | READ cluster1.* |

## Топики

| Топик | Кластер | RF | Partitions | Производитель | Потребитель |
|---|---|---|---|---|---|
| products-raw | 1 | 3 | 3 | shop-api | stream-processor |
| products-filtered | 1 | 3 | 3 | stream-processor | kafka-connect |
| client-events | 1 | 3 | 3 | client-api | mirrormaker → cluster2 |
| cluster1.client-events | 2 | 1 | 3 | mirrormaker | spark |
| recommendations | 2 | 1 | 1 | spark | client-api |

## Важные технические решения

**KRaft + StandardAuthorizer дедлок (Kafka 3.7)**

В combined mode (broker+controller на одном узле) StandardAuthorizer создаёт дедлок при старте:
авторизатор ждёт KRaft metadata log, а KRaft не может сформировать кворум без авторизатора.
Свойство `early.start.listeners` которое разрывает этот цикл появилось только в Kafka 3.8+.

Решение: авторизатор отключён. Клиенты по-прежнему **обязаны аутентифицироваться** через
SASL/SCRAM-SHA-512. ACL-правила прописаны в `setup.sh` как демонстрация концепции.

**MirrorMaker 2 — RF=1 для кластера 2**

cluster-2 содержит один брокер, поэтому для внутренних топиков Connect задан `replication.factor=1`.
Обратная репликация отключена (`cluster2->cluster1.enabled = false`).

**Healthcheck — hostname вместо localhost**

TLS сертификаты выпущены для `kafka-1`/`kafka-2`/`kafka-3` — не для `localhost`.
Healthcheck обращается к `kafka-1:9092` (не `localhost:9092`), иначе TLS отклоняет соединение.

## Статус реализации

| Шаг | Что сделано | Статус |
|-----|-------------|--------|
| 1. Источники | SHOP API, CLIENT API (Go + sarama + SCRAM) | ✅ запущено и проверено |
| 2. Kafka | Кластер 1 (3 брокера, KRaft, SASL_SSL, JMX) + Кластер 2 + MM2 | ✅ запущено и проверено |
| 3. Аналитика (базовый) | PySpark Structured Streaming: client-events → recommendations | ✅ запущено, данные в топике |
| 4. Stream Processor | Фильтрация tobacco/alcohol/weapons (Go); 2400+ записей в products-filtered | ✅ запущено и проверено |
| 5. Хранилище (базовый) | Kafka Connect FileStreamSink: 2404 строки в products-filtered.jsonl | ✅ запущено и проверено |
| 6. Мониторинг | Prometheus (kafka-1/2/3 JMX up), Grafana, Alertmanager → Telegram | ✅ запущено и проверено |

## Известные ограничения локального запуска

**Spark OOM на macOS (Exit 137)**

PySpark + 3 брокера Kafka одновременно требуют > 6 ГБ RAM. На MacBook с ограниченной
памятью Docker Desktop Spark может быть убит OOM killer'ом. Рекомендации в топике
`recommendations` при этом уже записаны до остановки.

**kafka-analytics JMX не настроен**

cluster-2 брокер (`kafka-analytics`) не имеет JMX Exporter — порт 9104 не открыт.
Prometheus показывает `kafka-analytics: down`. Для учебного проекта достаточно
мониторинга основного кластера (kafka-1/2/3).

**Go-приложения запускаются в Docker (не на хосте)**

Из-за особенностей маппинга портов в Docker на macOS (`/etc/hosts` не разрешает
отдельные порты для kafka-1/2/3), Go-приложения запускаются в Docker-контейнерах
в `kafka-cluster-net`. Для продакшна — отдельные образы с Dockerfile.
