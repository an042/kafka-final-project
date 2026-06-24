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

> Будет заполнен по мере реализации шагов.

1. Шаг 2: поднять Kafka кластеры (`step-2-kafka/`)
2. Шаг 1: запустить SHOP API и CLIENT API (`step-1-sources/`)
3. Шаг 4: запустить stream processor (`step-4-stream-processor/`)
4. Шаг 5: запустить хранилище (`step-5-storage/`)
5. Шаг 3: запустить HDFS + Spark (`step-3-analytics/basic/`)
6. Шаг 6: запустить мониторинг (`step-6-monitoring/`)

## Статус реализации

| Шаг | Базовый | Расширенный |
|-----|---------|-------------|
| 1. Источники данных | 🔲 | — |
| 2. Kafka | 🔲 | — |
| 3. Аналитика | 🔲 | 🔲 |
| 4. Stream processor | 🔲 | — |
| 5. Хранилище | 🔲 | 🔲 |
| 6. Мониторинг | 🔲 | 🔲 Telegram |
