#!/bin/bash
# generate-certs.sh — генерирует TLS-сертификаты для Kafka кластера.
#
# Что создаётся:
#   ca.key + ca.crt          — корневой CA (Certificate Authority)
#   kafka-N.keystore.jks     — keystore каждого брокера (приватный ключ + сертификат)
#   kafka.truststore.jks     — truststore (содержит ca.crt, используется всеми)
#
# Почему JKS, а не PKCS12:
#   bitnami/kafka по умолчанию ожидает JKS для SSL.
#   В PW7 мы использовали PKCS12 для NiFi/Java — там был trustedCertEntry.
#   Здесь оба формата работают, но JKS — стандарт для Kafka-брокеров.
#
# Запуск: bash generate-certs.sh
# Результат: папка ssl/certs/ со всеми файлами

# Прерываемся при любой ошибке, чтобы не получить частично созданные сертификаты
set -e

# Директория, куда кладём результаты (рядом с этим скриптом)
CERTS_DIR="$(dirname "$0")/certs"

# Пароль для всех keystore/truststore (одинаковый для простоты в учебном проекте)
PASS="TrustPass1"

# Срок действия сертификатов — 365 дней
VALIDITY=365

# Брокеры, для которых генерируем сертификаты
# kafka-analytics — единственный брокер кластера 2
BROKERS="kafka-1 kafka-2 kafka-3 kafka-analytics"

echo "==> Создаём директорию $CERTS_DIR"
mkdir -p "$CERTS_DIR"

# ─── 1. Корневой CA ────────────────────────────────────────────────────────────
# CA — это «доверенный центр», которому доверяют все участники.
# Брокеры предъявляют сертификаты, подписанные этим CA.
# Клиенты проверяют через truststore, что сертификат брокера подписан нашим CA.

echo ""
echo "==> Шаг 1: Генерируем корневой CA"

# Создаём приватный ключ CA (2048 бит RSA)
openssl genrsa -out "$CERTS_DIR/ca.key" 2048

# Создаём самоподписанный сертификат CA на основе ключа
# -x509    — формат сертификата X.509
# -new     — новый запрос на сертификат
# -days    — срок действия
# -subj    — subject без интерактивного ввода
openssl req -x509 -new -nodes \
  -key "$CERTS_DIR/ca.key" \
  -days "$VALIDITY" \
  -out "$CERTS_DIR/ca.crt" \
  -subj "/CN=KafkaCA/OU=kafka/O=practicum/L=Moscow/C=RU"

echo "    CA создан: ca.key + ca.crt"

# ─── 2. Truststore ────────────────────────────────────────────────────────────
# Truststore — хранилище доверенных сертификатов.
# Все брокеры и клиенты используют один и тот же truststore: он содержит ca.crt.
# Когда брокер получает соединение, он проверяет сертификат клиента/другого брокера
# через truststore — подписан ли он нашим CA?

echo ""
echo "==> Шаг 2: Создаём общий truststore"

# Импортируем CA-сертификат в truststore
# -importcert  — добавить сертификат
# -alias       — имя записи в хранилище
# -noprompt    — не спрашивать подтверждение
# -trustcacerts — пометить как доверенный CA
keytool -importcert \
  -alias ca-cert \
  -file "$CERTS_DIR/ca.crt" \
  -keystore "$CERTS_DIR/kafka.truststore.jks" \
  -storepass "$PASS" \
  -noprompt \
  -trustcacerts

echo "    Truststore создан: kafka.truststore.jks"

# ─── 3. Keystore для каждого брокера ──────────────────────────────────────────
# Keystore — личное хранилище брокера: приватный ключ + сертификат.
# Сертификат подписан нашим CA, поэтому другие участники ему доверяют.
# CN (Common Name) = имя хоста брокера (например, kafka-1).
# Это важно: при подключении клиент проверяет, что CN совпадает с хостом.

echo ""
echo "==> Шаг 3: Генерируем keystore для каждого брокера"

for BROKER in $BROKERS; do
  echo "    Обрабатываем $BROKER..."

  # Генерируем приватный ключ и самоподписанный сертификат для брокера
  # Помещаем сразу в keystore (keytool genkeypair делает это за один шаг)
  keytool -genkeypair \
    -alias "$BROKER" \
    -keyalg RSA \
    -keysize 2048 \
    -validity "$VALIDITY" \
    -keystore "$CERTS_DIR/$BROKER.keystore.jks" \
    -storepass "$PASS" \
    -keypass "$PASS" \
    -dname "CN=$BROKER,OU=kafka,O=practicum,L=Moscow,C=RU"

  # Экспортируем CSR (Certificate Signing Request) — запрос на подпись нашим CA
  keytool -certreq \
    -alias "$BROKER" \
    -keystore "$CERTS_DIR/$BROKER.keystore.jks" \
    -storepass "$PASS" \
    -file "$CERTS_DIR/$BROKER.csr"

  # Подписываем CSR нашим CA → получаем подписанный сертификат брокера
  openssl x509 -req \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -in "$CERTS_DIR/$BROKER.csr" \
    -days "$VALIDITY" \
    -out "$CERTS_DIR/$BROKER.crt"

  # Сначала импортируем CA в keystore брокера (цепочка доверия)
  keytool -importcert \
    -alias ca-cert \
    -file "$CERTS_DIR/ca.crt" \
    -keystore "$CERTS_DIR/$BROKER.keystore.jks" \
    -storepass "$PASS" \
    -noprompt \
    -trustcacerts

  # Затем импортируем подписанный сертификат брокера (заменяет самоподписанный)
  keytool -importcert \
    -alias "$BROKER" \
    -file "$CERTS_DIR/$BROKER.crt" \
    -keystore "$CERTS_DIR/$BROKER.keystore.jks" \
    -storepass "$PASS" \
    -noprompt

  # CSR больше не нужен — удаляем
  rm "$CERTS_DIR/$BROKER.csr"

  echo "    $BROKER.keystore.jks создан"
done

echo ""
echo "==> Готово! Содержимое $CERTS_DIR:"
ls -1 "$CERTS_DIR"
echo ""
echo "Пароль для всех хранилищ: $PASS"
echo "Не забудь добавить certs/ в .gitignore — там приватные ключи!"
