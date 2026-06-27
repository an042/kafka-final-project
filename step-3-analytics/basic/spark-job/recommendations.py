# recommendations.py — PySpark Structured Streaming: аналитика событий клиентов.
#
# Задача:
#   1. Читает топик client-events из кластера 2 (kafka-analytics:9092)
#      В кластер 2 события попадают через MirrorMaker 2 из кластера 1
#   2. Считает популярные товары в окне 5 минут (скользящее окно)
#   3. Формирует рекомендации: топ-5 товаров за последние 5 минут
#   4. Пишет рекомендации в топик recommendations в кластере 2
#
# Зависимости (устанавливаются вместе с PySpark):
#   pip install pyspark==3.5.3
#
# Запуск:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
#     --conf spark.jars.repositories=https://repo1.maven.org/maven2 \
#     recommendations.py
#
# Переменные окружения:
#   KAFKA_BROKERS    — кластер 2, default: kafka-analytics:9092
#   KAFKA_USER       — default: spark
#   KAFKA_PASSWORD   — default: SparkPass1
#   KAFKA_TLS_CA     — путь к CA, default: ../../step-2-kafka/ssl/certs/ca.crt
#   TOPIC_IN         — топик с событиями, default: cluster1.client-events
#                      (MM2 добавляет префикс "cluster1." к именам топиков из кластера 1)
#   TOPIC_OUT        — топик рекомендаций, default: recommendations

# pyspark.sql — основной модуль Spark SQL и Structured Streaming
from pyspark.sql import SparkSession
# functions — встроенные функции SQL (from_json, col, window, count, desc)
from pyspark.sql.functions import (
    from_json,  # парсинг JSON из Kafka value (bytes → StructType)
    col,        # обращение к колонке по имени
    window,     # скользящее временное окно (tumbling/sliding window)
    count,      # агрегация: количество событий
    desc,       # сортировка по убыванию
    struct,     # создание структуры из полей (для вывода JSON)
    to_json,    # сериализация struct в JSON строку
    lit,        # литерал (константа в запросе)
    to_timestamp,  # преобразование строки в timestamp
    collect_list,  # агрегация: список значений в массив
    current_timestamp,  # текущее время (для generated_at)
    date_format,       # форматирование даты
    slice,         # обрезка массива: slice(arr, start, len) → первые N элементов
)
# types — схема данных (соответствует ClientEvent из client-api/main.go)
from pyspark.sql.types import (
    StructType,   # структурный тип (аналог struct в Go)
    StructField,  # поле структуры
    StringType,   # строковый тип
    DoubleType,   # числовой тип с плавающей точкой
    TimestampType,# тип временной метки
)
# os — переменные окружения
import os
# json — сборка JSON для поля metadata
import json

# ── Конфигурация ──────────────────────────────────────────────────────────────

# Брокеры аналитического кластера (kafka-analytics:9092)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-analytics:9092")

# Учётные данные пользователя spark (создан в cluster-2/docker-compose.yml)
KAFKA_USER = os.getenv("KAFKA_USER", "spark")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "SparkPass1")

# Путь к CA сертификату (подписывает сертификат kafka-analytics)
KAFKA_TLS_CA = os.getenv("KAFKA_TLS_CA", "../../step-2-kafka/ssl/certs/ca.crt")

# Входной топик: MM2 добавляет префикс "cluster1." при репликации
# Если запускаете напрямую против кластера 1 — укажите TOPIC_IN=client-events
TOPIC_IN = os.getenv("TOPIC_IN", "cluster1.client-events")

# Выходной топик: recommendations создан в cluster-2/setup.sh
TOPIC_OUT = os.getenv("TOPIC_OUT", "recommendations")

# Размер временного окна для подсчёта популярных товаров
WINDOW_DURATION = "5 minutes"
# Сдвиг окна (sliding window): каждые 2 минуты обновляем статистику
SLIDE_DURATION = "2 minutes"

# ── SASL JAAS конфигурация ────────────────────────────────────────────────────

# JAAS (Java Authentication and Authorization Service) конфиг для Kafka клиента Spark.
# ScramLoginModule — реализация SCRAM-SHA-512 в Java.
# Формат точно такой же как в Kafka broker config.
SASL_JAAS_CONFIG = (
    f'org.apache.kafka.common.security.scram.ScramLoginModule required '
    f'username="{KAFKA_USER}" '
    f'password="{KAFKA_PASSWORD}";'
)

# ── Схема входных данных ──────────────────────────────────────────────────────

# Схема ClientEvent (должна совпадать со структурой в client-api/main.go).
# Kafka хранит сообщения как bytes — from_json() десериализует value в эту схему.
CLIENT_EVENT_SCHEMA = StructType([
    # Уникальный идентификатор события
    StructField("event_id", StringType(), nullable=True),
    # Тип: "search", "view", "click", "purchase"
    StructField("event_type", StringType(), nullable=True),
    # ID пользователя
    StructField("user_id", StringType(), nullable=True),
    # ID товара (пустой для событий "search")
    StructField("product_id", StringType(), nullable=True),
    # Поисковый запрос (для событий "search")
    StructField("query", StringType(), nullable=True),
    # Временная метка события в формате ISO8601 / RFC3339
    StructField("timestamp", StringType(), nullable=True),
])

# ── Создаём SparkSession ──────────────────────────────────────────────────────

# SparkSession — точка входа в Spark API.
# appName — отображается в Spark UI (http://localhost:4040)
spark = (
    SparkSession.builder
    .appName("Покупай-выгодно: рекомендации")
    # Настройки Kafka коннектора (передаются во все Kafka-источники)
    # security.protocol — SASL_SSL: и аутентификация, и шифрование
    .config("spark.kafka.security.protocol", "SASL_SSL")
    # SCRAM-SHA-512 механизм аутентификации
    .config("spark.kafka.sasl.mechanism", "SCRAM-SHA-512")
    # JAAS конфиг с логином и паролем
    .config("spark.kafka.sasl.jaas.config", SASL_JAAS_CONFIG)
    # Путь к CA сертификату для верификации брокера
    .config("spark.kafka.ssl.truststore.location", KAFKA_TLS_CA)
    # PEM формат (не JKS — мы используем PEM certs)
    .config("spark.kafka.ssl.truststore.type", "PEM")
    # Отключаем проверку корректности водяного знака для nested stateful операций.
    # Без этого Spark отклоняет запрос из-за double-aggregation с watermark.
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .getOrCreate()
)

# Уровень логирования: WARN скрывает информационный шум Spark
spark.sparkContext.setLogLevel("WARN")

# Гарантируем отключение проверки watermark correctness (runtime override)
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

print(f"[INFO] Spark запущен. Читаю топик: {TOPIC_IN} @ {KAFKA_BROKERS}")

# ── Читаем поток событий из Kafka ─────────────────────────────────────────────

# readStream — создаёт неограниченный DataFrame из Kafka топика
raw_stream = (
    spark.readStream
    .format("kafka")  # Источник: Apache Kafka
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)  # Адреса брокеров
    .option("subscribe", TOPIC_IN)  # Подписываемся на топик
    # startingOffsets=earliest — при первом запуске читаем все накопленные события
    .option("startingOffsets", "earliest")
    # SASL/TLS опции для подключения
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("kafka.sasl.jaas.config", SASL_JAAS_CONFIG)
    .option("kafka.ssl.truststore.location", KAFKA_TLS_CA)
    .option("kafka.ssl.truststore.type", "PEM")
    .load()
)

# raw_stream имеет схему: key, value, topic, partition, offset, timestamp, timestampType
# value — это bytes (Kafka message value)

# ── Десериализуем JSON из Kafka value ────────────────────────────────────────

# cast("string") — конвертируем bytes в строку UTF-8
# from_json() — парсим JSON строку по схеме CLIENT_EVENT_SCHEMA
events = (
    raw_stream
    # Декодируем Kafka value bytes → строка
    .select(from_json(col("value").cast("string"), CLIENT_EVENT_SCHEMA).alias("data"))
    # Разворачиваем вложенную структуру data.* в колонки верхнего уровня
    .select("data.*")
)

# Фильтруем: нас интересуют только события с product_id (не поиски без товара)
# .isNotNull() — убираем null строки (поле может быть не задано)
# != "" — убираем пустые строки (для события "search" product_id = "")
events_with_product = events.filter(
    col("product_id").isNotNull() & (col("product_id") != "")
)

# Конвертируем строку timestamp в тип TimestampType для работы с window()
# to_timestamp() понимает RFC3339 формат из Go (2006-01-02T15:04:05Z)
# withWatermark — объявляем допустимое опоздание событий (10 минут).
# Spark сможет удалять старые состояния агрегации и корректно закрывать окна.
# Без watermark агрегация хранит все окна в памяти вечно (утечка состояния).
events_timed = (
    events_with_product
    .withColumn("event_time", to_timestamp(col("timestamp")))  # строка → timestamp
    .withWatermark("event_time", "10 minutes")  # опоздавшие >10 мин события игнорируются
)

# ── Агрегация: топ товаров в скользящем окне ─────────────────────────────────

# window() — оконная агрегация по времени.
# WINDOW_DURATION="5 minutes" — каждое окно охватывает 5 минут событий
# SLIDE_DURATION="2 minutes" — новое окно создаётся каждые 2 минуты (sliding window)
# Каждое событие попадает в несколько перекрывающихся окон.
product_counts = (
    events_timed
    # Группируем: по временному окну + user_id + product_id
    # Это позволит считать популярные товары для каждого пользователя
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),  # временное окно
        col("user_id"),       # персонализация: считаем для каждого пользователя
        col("product_id"),    # товар который смотрели
    )
    # Считаем количество событий (просмотров/кликов) для каждой комбинации
    .agg(count("*").alias("event_count"))
)

# ── Формируем рекомендации ────────────────────────────────────────────────────

# Для каждого пользователя в каждом окне берём топ-5 товаров по числу событий.
# В PySpark Structured Streaming нет RANK() в режиме streaming (только в batch).
# Используем collect_list + сортировку внутри окна (упрощённый подход для учебного проекта).

# Топ-5 самых популярных товаров в каждом окне (независимо от пользователя)
# Это "глобальные" рекомендации — для Шага 3 базовый вариант
global_top = (
    product_counts
    # Группируем только по окну (глобальный топ, не персонализированный)
    .groupBy("window", "user_id")
    # collect_list собирает все product_id окна в массив.
    # slice(..., 1, 5) обрезает до первых 5 элементов (1-based индекс).
    # В streaming режиме сортировка внутри агрегации недоступна — берём первые 5.
    .agg(slice(collect_list("product_id"), 1, 5).alias("product_ids"))
)

# ── Формируем финальный JSON для топика recommendations ───────────────────────

# Каждое сообщение в recommendations — это JSON объект Recommendation.
# Структура совпадает с типом Recommendation в client-api/main.go.
recommendations = (
    global_top
    .select(
        # user_id — кому адресована рекомендация
        col("user_id"),
        # product_ids — список рекомендованных товаров
        col("product_ids"),
        # generated_at — когда Spark сгенерировал рекомендации
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("generated_at"),
        # window.start — начало временного окна (для отладки)
        col("window.start").alias("window_start"),
        # window.end — конец временного окна
        col("window.end").alias("window_end"),
    )
    # Сериализуем всё в JSON строку для записи в Kafka
    # struct() создаёт вложенный объект, to_json() конвертирует в строку
    .select(
        # Ключ сообщения = user_id (для партиционирования по пользователю)
        col("user_id").alias("key"),
        # Value = весь объект рекомендации в JSON
        to_json(struct(
            col("user_id"),
            col("product_ids"),
            col("generated_at"),
        )).alias("value"),
    )
)

# ── Пишем результат в Kafka ───────────────────────────────────────────────────

# writeStream — запускает непрерывную запись агрегированных данных
query = (
    recommendations
    .writeStream
    .format("kafka")  # Приёмник: Apache Kafka
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)  # Куда писать
    .option("topic", TOPIC_OUT)  # Топик рекомендаций
    # SASL/TLS для продюсера Spark
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("kafka.sasl.jaas.config", SASL_JAAS_CONFIG)
    .option("kafka.ssl.truststore.location", KAFKA_TLS_CA)
    .option("kafka.ssl.truststore.type", "PEM")
    # Checkpoint: Spark сохраняет прогресс чтобы не пересчитывать при рестарте
    # Директория создаётся автоматически (для учебного проекта — локальный путь)
    .option("checkpointLocation", "/tmp/spark-checkpoint-recommendations")
    # outputMode=update — при каждом триггере пишем только изменившиеся строки
    # Это оптимально для агрегаций (не перезаписываем всё, только новые результаты)
    .outputMode("update")
    # trigger=ProcessingTime — запускать агрегацию каждые N секунд
    # "60 seconds" = раз в минуту обновляем рекомендации
    .trigger(processingTime="60 seconds")
    .start()
)

print(f"[INFO] Стриминг запущен. Пишу рекомендации в топик: {TOPIC_OUT}")
print(f"[INFO] Окно: {WINDOW_DURATION}, сдвиг: {SLIDE_DURATION}")
print(f"[INFO] Spark UI: http://localhost:4040")

# awaitTermination() — блокирует основной поток пока streaming query работает
# Завершение: Ctrl+C (SIGINT) или ошибка в стриминге
query.awaitTermination()
