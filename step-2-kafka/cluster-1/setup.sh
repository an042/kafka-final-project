#!/bin/bash
# setup.sh — создаёт топики и ACL в кластере 1 после его запуска.
#
# Запускать ПОСЛЕ: docker compose up -d (подождать 30-60 секунд)
#
# Что создаётся:
#   Топики:
#     products-raw       — сырые товары от SHOP API
#     products-filtered  — отфильтрованные от Stream Processor
#     client-events      — события клиентов от CLIENT API
#
#   ACL (демонстрационно — в Kafka 3.7 авторизатор отключён):
#     shop-api          → WRITE products-raw
#     stream-processor  → READ products-raw, WRITE products-filtered
#     client-api        → WRITE client-events
#     mirrormaker       → READ/DESCRIBE всё (для репликации в кластер 2)
#     kafka-connect     → READ products-filtered (для Шага 5)
#
# Примечание: если запускаете с хост-машины, добавьте в /etc/hosts:
#   127.0.0.1 kafka-1 kafka-2 kafka-3
# Это нужно т.к. Kafka отдаёт advertised_listeners = kafka-1/2/3 при handshake.

# set -e включён только для критических команд (топики).
# ACL-команды нефатальны — авторизатор отключён в Kafka 3.7 (см. docker-compose.yml).
set -e

# ─── Шаг 1: Записываем admin.properties внутрь контейнера ─────────────────────
# Проблема: setup.sh запускается на хосте, но kafka-инструменты работают внутри
# контейнера. Решение: передаём файл конфигурации через stdin с docker exec -i.
# echo "$VAR" | docker exec -i kafka-1 bash -c 'cat > /tmp/file' — пишет на месте.

echo "==> Записываем admin.properties в контейнер kafka-1..."

# Многострочная переменная с конфигом SASL_SSL для admin
# Строки JAAS: username и password в двойных кавычках — требование формата
ADMIN_PROPS='security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="AdminPass1";
ssl.truststore.location=/bitnami/kafka/config/certs/kafka.truststore.pem
ssl.truststore.type=PEM'

# Передаём через stdin (-i = --interactive), пишем файл внутри контейнера
echo "$ADMIN_PROPS" | docker exec -i kafka-1 bash -c 'cat > /tmp/admin.properties'
echo "    ОК: /tmp/admin.properties создан в kafka-1"

# ─── Шаг 2: Ждём готовности кластера ─────────────────────────────────────────
echo ""
echo "==> Ждём готовности кластера (до 90 секунд)..."

MAX_WAIT=90
WAITED=0

# kafka-broker-api-versions.sh возвращает 0 когда брокер принял SASL-handshake
until docker exec kafka-1 kafka-broker-api-versions.sh \
        --bootstrap-server kafka-1:9092 \
        --command-config /tmp/admin.properties \
        > /dev/null 2>&1; do
  WAITED=$((WAITED + 5))
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "ОШИБКА: кластер не поднялся за ${MAX_WAIT}с. Проверьте: docker compose logs"
    exit 1
  fi
  echo "    Ожидание... ${WAITED}/${MAX_WAIT}с"
  sleep 5
done

echo "    ОК: кластер готов!"

# ─── Шаг 3: Создание топиков ───────────────────────────────────────────────────
echo ""
echo "==> Создаём топики..."

# Функция для создания топика — убирает дублирование кода
# Аргументы: $1 = имя топика
create_topic() {
  local TOPIC=$1
  # --if-not-exists — идемпотентность: повторный запуск не ломается
  docker exec kafka-1 kafka-topics.sh \
    --create \
    --bootstrap-server kafka-1:9092 \
    --command-config /tmp/admin.properties \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 3 \
    --config retention.ms=604800000 \
    --if-not-exists
  echo "    ✓ $TOPIC"
}

# products-raw: сырые данные о товарах от SHOP API (до фильтрации)
create_topic "products-raw"

# products-filtered: товары после фильтрации запрещённых (Stream Processor → Kafka Connect)
create_topic "products-filtered"

# client-events: поисковые запросы и действия пользователей
create_topic "client-events"

# ─── Шаг 4: ACL (демонстрация концепции) ─────────────────────────────────────
# ВАЖНО: в текущей конфигурации (Kafka 3.7, авторизатор отключён) эти ACL
# не применяются. Команды оставлены для изучения синтаксиса kafka-acls.sh.
#
# В Kafka 3.8+ с включённым StandardAuthorizer эти ACL заработают:
#   - clients обязаны аутентифицироваться (SASL) И иметь явное разрешение (ACL)
#   - без разрешения доступ запрещён (ALLOW_EVERYONE_IF_NO_ACL_FOUND=false)

echo ""
echo "==> Настраиваем ACL (демонстрация — авторизатор отключён в Kafka 3.7)..."

# Обёртка: выполняет ACL-команду, при ошибке выводит предупреждение, не падает.
# if-форма важна: с set -e, присваивание "OUTPUT=$(cmd)" прерывает скрипт если cmd падает.
# Форма "if OUTPUT=$(cmd)" не прерывает — if сам обрабатывает exit code.
run_acl() {
  # "$@" — все переданные аргументы
  if docker exec kafka-1 "$@" > /dev/null 2>&1; then
    echo "ОК"
  else
    # SecurityDisabledException: No Authorizer is configured — ожидаемое поведение
    echo "(пропущено — нет авторизатора, Kafka 3.7 combined mode)"
  fi
}

# ACL в Kafka работает по принципу белого списка:
# явно разрешаем только то, что нужно каждому пользователю.

# shop-api пишет товары в products-raw
# --operation Write = PRODUCE
echo -n "    shop-api → WRITE products-raw: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:shop-api" \
  --operation Write \
  --topic "products-raw"

# stream-processor читает сырые товары
echo -n "    stream-processor → READ products-raw: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:stream-processor" \
  --operation Read \
  --topic "products-raw"

# Доступ к consumer group — Kafka требует ACL на группу отдельно от топика
# Без этого consumer group API (commit offsets, join group) вернёт AuthorizationException
echo -n "    stream-processor → READ group:stream-processor-group: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:stream-processor" \
  --operation Read \
  --group "stream-processor-group"

# stream-processor пишет отфильтрованные товары
echo -n "    stream-processor → WRITE products-filtered: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:stream-processor" \
  --operation Write \
  --topic "products-filtered"

# client-api пишет события (поиски, просмотры) для аналитики
echo -n "    client-api → WRITE client-events: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:client-api" \
  --operation Write \
  --topic "client-events"

# mirrormaker читает ВСЕ топики и группы (wildcard '*')
# Это необходимо для репликации, включая служебные топики MM2
echo -n "    mirrormaker → READ/DESCRIBE all topics: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:mirrormaker" \
  --operation Read \
  --operation Describe \
  --topic "*"

echo -n "    mirrormaker → READ all groups: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:mirrormaker" \
  --operation Read \
  --group "*"

# kafka-connect читает отфильтрованные товары
echo -n "    kafka-connect → READ products-filtered: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:kafka-connect" \
  --operation Read \
  --topic "products-filtered"

echo -n "    kafka-connect → READ group:connect-group: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:kafka-connect" \
  --operation Read \
  --group "connect-group"

# Kafka Connect нужны внутренние топики для хранения конфигурации и оффсетов
echo -n "    kafka-connect → READ/WRITE connect internal topics: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:kafka-connect" \
  --operation Write \
  --operation Read \
  --operation Describe \
  --topic "connect-configs" \
  --topic "connect-offsets" \
  --topic "connect-status"

# ─── Шаг 5: Проверка ─────────────────────────────────────────────────────────
echo ""
echo "==> Список топиков в кластере:"
docker exec kafka-1 kafka-topics.sh \
  --list \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties

# Удаляем временный файл конфигурации из контейнера
docker exec kafka-1 rm -f /tmp/admin.properties

echo ""
echo "==> Готово! Кластер 1 полностью настроен."
echo ""
echo "Следующие шаги:"
echo "  1. cd ../cluster-2 && docker compose up -d"
echo "  2. bash setup.sh (в cluster-2/)"
echo "  3. cd ../mirrormaker && docker compose up -d"
