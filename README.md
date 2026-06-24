# Финальный проект — Аналитическая платформа маркетплейса

Яндекс Практикум · Курс Apache Kafka

## Бизнес-контекст

«Покупай выгодно» — маркетплейс, которому нужна аналитическая платформа:
собирать данные о товарах и клиентах, фильтровать запрещённые товары,
строить персонализированные рекомендации и следить за состоянием системы.

## Архитектура

```
SHOP API (Go)
    │ товары (JSON)
    ▼
Kafka кластер 1 ──── TLS + ACL ────► Stream Processor (Goka/Go)
    │                                   │ фильтрует запрещённые товары
    │ MirrorMaker 2                     ▼
    ▼                               Kafka Connect
Kafka кластер 2                         │
    │                               ┌───┴────────────┐
    │                               │ basic: файл    │ extended: Elasticsearch
    ▼                               └────────────────┘
HDFS (basic) / ksqlDB (extended)
    │
    ▼
Spark (PySpark) → топик recommendations в Kafka
    ▲
CLIENT API (Go) ──────────────────► Elasticsearch (поиск товаров)
                                    Kafka (аналитика запросов)

Мониторинг: JMX Exporter → Prometheus → Grafana + Alertmanager → Telegram
```

## Стек технологий

| Компонент | Технология | Обоснование выбора |
|---|---|---|
| SHOP API, CLIENT API | Go | Продолжаем стек предыдущих работ |
| Stream Processor | Go + Goka | Goka — Go-нативная библиотека для Kafka Streams; не переключаем язык |
| Spark-задача | Python (PySpark) | Spark не поддерживает Go; PySpark — стандарт для аналитики на Spark |
| Хранилище (расширенный) | Elasticsearch | Поиск товаров по имени — родная задача Elasticsearch; PostgreSQL потребовал бы ручного написания индексов |
| Мессенджер алертов | Telegram | Бот уже настроен в инфраструктуре |

## Структура

```
.
├── step-1-sources/             # Шаг 1: Источники данных
│   ├── shop-api/               # SHOP API: читает products.json → Kafka
│   │   ├── main.go
│   │   ├── go.mod
│   │   └── data/
│   │       └── products.json   # Тестовые товары (под контролем версий)
│   └── client-api/             # CLIENT API: поиск товаров + рекомендации
│       ├── main.go
│       └── go.mod
│
├── step-2-kafka/               # Шаг 2: Kafka, TLS, ACL, репликация, MirrorMaker
│   ├── cluster-1/              # Основной кластер (от магазинов)
│   ├── cluster-2/              # Зеркало для аналитики
│   ├── ssl/                    # TLS сертификаты и скрипты генерации
│   └── mirrormaker/            # MirrorMaker 2 — дублирование между кластерами
│
├── step-3-analytics/           # Шаг 3: Аналитическая система
│   ├── basic/                  # ОБЯЗАТЕЛЬНО: HDFS + Spark → рекомендации в Kafka
│   │   ├── hdfs-consumer/      # Go: читает из Kafka кластер-2, пишет в HDFS
│   │   └── spark-job/          # PySpark: читает из HDFS, пишет рекомендации в Kafka
│   └── extended/               # РАСШИРЕННЫЙ: real-time через ksqlDB
│
├── step-4-stream-processor/    # Шаг 4: Goka — фильтрация запрещённых товаров
│   ├── main.go                 # Читает из Kafka, фильтрует, пишет в выходной топик
│   ├── go.mod
│   └── banned-products.json    # Список запрещённых товаров (управляется через CLI)
│
├── step-5-storage/             # Шаг 5: Хранилище данных
│   ├── basic/                  # ОБЯЗАТЕЛЬНО: Kafka Connect → файл (отладка)
│   └── extended/               # РАСШИРЕННЫЙ: Kafka Connect → Elasticsearch
│
├── step-6-monitoring/          # Шаг 6: Мониторинг
│   ├── prometheus/             # Конфиг Prometheus + JMX Exporter
│   ├── jmx-exporter/          # Агент сбора метрик JVM/Kafka
│   ├── grafana/dashboards/     # Дашборд Kafka метрик
│   ├── alertmanager/           # Алерты при падении брокера
│   └── telegram/               # Webhook для уведомлений в Telegram
│
└── README.md                   # Этот файл
```

## Порядок запуска

### Шаг 2: Kafka кластеры (выполнен)

```bash
# 1. Генерируем TLS сертификаты для всех брокеров и клиентов
cd step-2-kafka/ssl
bash generate-certs.sh
# Создаёт: ssl/certs/ — CA, keystore для kafka-1/2/3/analytics, truststore

# 2. Запускаем кластер 1 (3 брокера, KRaft, SASL_SSL, ACL)
cd ../cluster-1
docker compose up -d
# Ждём 30-60 секунд, затем:
bash setup.sh     # Создаёт топики и ACL

# 3. Запускаем кластер 2 (1 брокер, аналитика)
cd ../cluster-2
docker compose up -d
bash setup.sh     # Создаёт ACL для аналитических пользователей

# 4. Запускаем MirrorMaker 2 (репликация cluster1 → cluster2)
cd ../mirrormaker
docker compose up -d
# После старта: топики cluster1.products-raw и др. появятся в кластере 2
```

**Пользователи и пароли (SCRAM-SHA-512):**

| Пользователь | Пароль | Кластер | Права |
|---|---|---|---|
| admin | AdminPass1 | 1, 2 | суперпользователь |
| shop-api | ShopApiPass1 | 1 | WRITE products-raw |
| stream-processor | StreamPass1 | 1 | READ products-raw, WRITE products-filtered |
| client-api | ClientApiPass1 | 1 | WRITE client-events |
| mirrormaker | MirrorPass1 | 1, 2 | READ/DESCRIBE all (репликация) |
| kafka-connect | ConnectPass1 | 1, 2 | READ products-filtered (Шаг 5) |
| analytics | AnalyticsPass1 | 2 | READ cluster1.* |
| spark | SparkPass1 | 2 | WRITE recommendations |

**Подключение из клиентских приложений (Go, Python):**

```
bootstrap.servers = kafka-1:9092,kafka-2:9092,kafka-3:9092  (кластер 1)
bootstrap.servers = kafka-analytics:9092                    (кластер 2)
security.protocol = SASL_SSL
sasl.mechanism = SCRAM-SHA-512
ssl.ca.location = step-2-kafka/ssl/certs/ca.crt   (PEM формат, используем CA напрямую)
```

> **Примечание про ACL:** StandardAuthorizer намеренно отключён в этой конфигурации.
> В Kafka 3.7 combined mode (broker+controller на одном узле) возникает дедлок при старте:
> авторизатор ждёт KRaft metadata log, а KRaft не может сформировать кворум без авторизатора.
> Решение (свойство `early.start.listeners`) появилось в Kafka 3.8+.
> Клиенты по-прежнему **обязаны аутентифицироваться** через SASL/SCRAM-SHA-512 — ACL-правила
> прописаны в setup.sh как демонстрация, но не применяются.

> **Подключение с хост-машины:** Добавьте в `/etc/hosts`:  
> `127.0.0.1 kafka-1 kafka-2 kafka-3 kafka-analytics`  
> Это нужно потому что Kafka сообщает клиентам advertised_listeners = Docker hostnames.

---

> Шаги 1, 3-6 будут добавлены по мере реализации.

1. Шаг 2: поднять Kafka кластеры (`step-2-kafka/`) — ✅ **выполнено**
2. Шаг 1: запустить SHOP API и CLIENT API (`step-1-sources/`)
3. Шаг 4: запустить stream processor (`step-4-stream-processor/`)
4. Шаг 5: запустить хранилище (`step-5-storage/`)
5. Шаг 3: запустить HDFS + Spark (`step-3-analytics/basic/`)
6. Шаг 6: запустить мониторинг (`step-6-monitoring/`)

## Статус реализации

| Шаг | Базовый | Расширенный |
|-----|---------|-------------|
| 1. Источники данных | 🔲 | — |
| 2. Kafka | ✅ | — |
| 3. Аналитика | 🔲 | 🔲 |
| 4. Stream processor | 🔲 | — |
| 5. Хранилище | 🔲 | 🔲 |
| 6. Мониторинг | 🔲 | 🔲 Telegram |
