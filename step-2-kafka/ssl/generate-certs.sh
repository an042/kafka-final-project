#!/bin/bash
# generate-certs.sh — генерирует TLS-сертификаты для Kafka кластера.
#
# Формат: PEM (не JKS)
# Почему PEM вместо JKS:
#   keytool требует Java Runtime на хосте — дополнительная зависимость.
#   bitnami/kafka поддерживает KAFKA_TLS_TYPE=PEM напрямую через переменные окружения.
#   openssl есть на macOS без дополнительной установки.
#
# Что создаётся в ssl/certs/:
#   ca.key, ca.crt                — корневой CA
#   kafka-N.key, kafka-N.crt     — ключ + сертификат каждого брокера (подписан CA)
#
# Как это работает с bitnami/kafka (KAFKA_TLS_TYPE=PEM):
#   KAFKA_CFG_SSL_KEYSTORE_KEY                  = kafka-N.key (приватный ключ)
#   KAFKA_CFG_SSL_KEYSTORE_CERTIFICATE_CHAIN    = kafka-N.crt + ca.crt (cert chain)
#   KAFKA_CFG_SSL_TRUSTSTORE_CERTIFICATES       = ca.crt (клиенты проверяют брокер через CA)
#
# Запуск: bash generate-certs.sh
# Результат: ssl/certs/ со всеми файлами

# Прерываемся при любой ошибке
set -e

# Директория рядом со скриптом
CERTS_DIR="$(dirname "$0")/certs"

# Срок действия 365 дней
VALIDITY=365

# Брокеры обоих кластеров
# kafka-analytics — единственный брокер кластера 2
BROKERS="kafka-1 kafka-2 kafka-3 kafka-analytics"

echo "==> Создаём директорию $CERTS_DIR"
mkdir -p "$CERTS_DIR"

# ─── 1. Корневой CA ────────────────────────────────────────────────────────────
# CA — самоподписанный, ему доверяют все брокеры и клиенты.
# ca.crt монтируется в каждый контейнер как "truststore" (список доверенных CA).

echo ""
echo "==> Шаг 1: Генерируем корневой CA"

# Генерируем приватный ключ CA (2048 бит RSA — достаточно для учебного проекта)
openssl genrsa -out "$CERTS_DIR/ca.key" 2048

# Самоподписанный сертификат CA
# -x509    — формат X.509 (стандарт TLS сертификатов)
# -new     — создаём новый
# -nodes   — приватный ключ без passphrase (удобно для автоматизации)
# -subj    — subject без интерактивного ввода
openssl req -x509 -new -nodes \
  -key "$CERTS_DIR/ca.key" \
  -days "$VALIDITY" \
  -out "$CERTS_DIR/ca.crt" \
  -subj "/CN=KafkaCA/OU=kafka/O=practicum/L=Moscow/C=RU"

echo "    ОК: ca.key + ca.crt"

# ─── 2. Сертификаты брокеров ──────────────────────────────────────────────────
# Для каждого брокера:
#   1. Генерируем приватный ключ
#   2. Создаём CSR (Certificate Signing Request) — запрос на подпись
#   3. Подписываем CSR нашим CA — получаем сертификат
#
# CN = Docker hostname брокера (kafka-1, kafka-2, etc.)
# SAN (subjectAltName) = DNS:kafka-N + IP:127.0.0.1
# Go 1.15+ требует SAN — CN без SAN отклоняется (RFC 2818). Это не настройка Go, это стандарт.

echo ""
echo "==> Шаг 2: Генерируем ключи и сертификаты брокеров"

for BROKER in $BROKERS; do
  echo "    Обрабатываем $BROKER..."

  # Приватный ключ брокера
  openssl genrsa -out "$CERTS_DIR/$BROKER.key" 2048

  # CSR (Certificate Signing Request): содержит публичный ключ и CN брокера
  # -new     — создаём новый CSR
  # -key     — на основе приватного ключа
  # -subj    — CN = Docker hostname брокера (важно для проверки TLS!)
  openssl req -new \
    -key "$CERTS_DIR/$BROKER.key" \
    -out "$CERTS_DIR/$BROKER.csr" \
    -subj "/CN=$BROKER/OU=kafka/O=practicum/L=Moscow/C=RU"

  # Подписываем CSR нашим CA → получаем подписанный сертификат брокера
  # -CAcreateserial — автоматически создаёт файл серийных номеров (ca.srl)
  # -extfile <(...) — добавляем SAN (Subject Alternative Names):
  #   DNS:$BROKER — Docker hostname (kafka-1 и т.д.)
  #   IP:127.0.0.1 — для соединений с хост-машины через маппинг портов
  # SAN обязателен: Go 1.15+ отклоняет сертификаты только с CN (RFC 2818)
  openssl x509 -req \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -in "$CERTS_DIR/$BROKER.csr" \
    -days "$VALIDITY" \
    -out "$CERTS_DIR/$BROKER.crt" \
    -extfile <(printf "subjectAltName=DNS:%s,IP:127.0.0.1\n" "$BROKER")

  # CSR больше не нужен после подписи
  rm "$CERTS_DIR/$BROKER.csr"

  # bitnami/kafka с KAFKA_TLS_TYPE=PEM ожидает certificate chain:
  # сначала сертификат брокера, затем CA сертификат (цепочка)
  # Создаём объединённый файл kafka-N.chain.crt
  cat "$CERTS_DIR/$BROKER.crt" "$CERTS_DIR/ca.crt" > "$CERTS_DIR/$BROKER.chain.crt"

  echo "      ОК: $BROKER.key + $BROKER.crt + $BROKER.chain.crt"
done

# Удаляем временный файл серийных номеров openssl
rm -f "$CERTS_DIR/ca.srl"

echo ""
echo "==> Готово! Содержимое $CERTS_DIR:"
ls -1 "$CERTS_DIR"
echo ""
echo "Файлы для монтирования в docker-compose:"
echo "  Broker key:   \$BROKER.key"
echo "  Cert chain:   \$BROKER.chain.crt (broker cert + CA cert)"
echo "  Truststore:   ca.crt (общий для всех)"
echo ""
echo "ВАЖНО: директория ssl/certs/ в .gitignore — не коммитить приватные ключи!"
