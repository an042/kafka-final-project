#!/bin/bash
# setup.sh — настраивает ACL в кластере 2 (аналитический кластер).
#
# Запускать ПОСЛЕ:
#   docker compose up -d (в cluster-2/)
#   bash ../cluster-1/setup.sh (настройка кластера 1)
#
# Топики в кластере 2:
#   cluster1.products-raw      — зеркало из кластера 1 (создаёт MirrorMaker)
#   cluster1.products-filtered — зеркало из кластера 1 (создаёт MirrorMaker)
#   cluster1.client-events     — зеркало из кластера 1 (создаёт MirrorMaker)
#   recommendations            — рекомендации от Spark (создаётся в Шаге 3)
#
# ACL:
#   analytics     → READ все зеркальные топики (для HDFS consumer в Шаге 3)
#   spark         → WRITE recommendations (Spark пишет рекомендации)
#   client-api    → READ recommendations (CLIENT API читает рекомендации)
#   kafka-connect → READ зеркальные топики (для хранения в Шаге 5)

set -e

# ACL-команды нефатальны — авторизатор отключён в Kafka 3.7.
# Используем run_acl() чтобы скрипт продолжался при ошибках ACL.
run_acl() {
  # if-форма: с set -e нельзя делать "OUTPUT=$(cmd)" напрямую
  if docker exec kafka-analytics "$@" > /dev/null 2>&1; then
    echo "ОК"
  else
    echo "(пропущено — нет авторизатора, Kafka 3.7 combined mode)"
  fi
}

# ─── Записываем admin.properties внутрь контейнера ─────────────────────────
echo "==> Записываем admin.properties в контейнер kafka-analytics..."

ADMIN_PROPS='security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="AdminPass1";
ssl.truststore.location=/bitnami/kafka/config/certs/kafka.truststore.pem
ssl.truststore.type=PEM'

# Передаём конфиг через stdin — тот же паттерн что в cluster-1/setup.sh
echo "$ADMIN_PROPS" | docker exec -i kafka-analytics bash -c 'cat > /tmp/admin.properties'
echo "    ОК"

# ─── Ждём готовности кластера 2 ───────────────────────────────────────────
echo ""
echo "==> Ждём готовности кластера 2 (до 90 секунд)..."

MAX_WAIT=90
WAITED=0

until docker exec kafka-analytics kafka-broker-api-versions.sh \
        --bootstrap-server kafka-analytics:9092 \
        --command-config /tmp/admin.properties \
        > /dev/null 2>&1; do
  WAITED=$((WAITED + 5))
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "ОШИБКА: кластер-2 не поднялся за ${MAX_WAIT}с"
    exit 1
  fi
  echo "    Ожидание... ${WAITED}/${MAX_WAIT}с"
  sleep 5
done
echo "    ОК: кластер-2 готов!"

# ─── Создаём топик recommendations ────────────────────────────────────────
# MirrorMaker создаёт зеркальные топики автоматически.
# recommendations — не зеркальный, его пишет Spark (Шаг 3).
# Создаём заранее чтобы Spark не тратил время на создание.
echo ""
echo "==> Создаём топик recommendations..."

docker exec kafka-analytics kafka-topics.sh \
  --create \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --topic "recommendations" \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists
echo "    ✓ recommendations"

# ─── ACL (демонстрация концепции) ─────────────────────────────────────────
echo ""
echo "==> Настраиваем ACL (демонстрация — авторизатор отключён в Kafka 3.7)..."

# analytics читает зеркальные данные для записи в HDFS и последующей обработки в Spark
# Топики называются cluster1.* — MirrorMaker добавляет префикс "cluster1."
echo -n "    analytics → READ cluster1.* (prefixed pattern): "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:analytics" \
  --operation Read \
  --operation Describe \
  --topic "cluster1.*" \
  --resource-pattern-type prefixed

# Consumer group для HDFS consumer
echo -n "    analytics → READ group:hdfs-consumer-group: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:analytics" \
  --operation Read \
  --group "hdfs-consumer-group"

# Spark пишет рекомендации в Kafka
echo -n "    spark → WRITE recommendations: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:spark" \
  --operation Write \
  --topic "recommendations"

# CLIENT API читает рекомендации для ответа пользователю
echo -n "    client-api → READ recommendations: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:client-api" \
  --operation Read \
  --topic "recommendations"

echo -n "    client-api → READ group:client-api-group: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:client-api" \
  --operation Read \
  --group "client-api-group"

# Kafka Connect в кластере 2 читает зеркальные данные для хранения (Шаг 5)
echo -n "    kafka-connect → READ cluster1.* (prefixed pattern): "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:kafka-connect" \
  --operation Read \
  --operation Describe \
  --topic "cluster1.*" \
  --resource-pattern-type prefixed

echo -n "    kafka-connect → READ group:connect-analytics-group: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:kafka-connect" \
  --operation Read \
  --group "connect-analytics-group"

# Kafka Connect нужны внутренние топики в кластере 2
echo -n "    kafka-connect → READ/WRITE connect internal topics: "
run_acl kafka-acls.sh \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal "User:kafka-connect" \
  --operation Write \
  --operation Read \
  --operation Describe \
  --topic "connect-configs" \
  --topic "connect-offsets" \
  --topic "connect-status"

# ─── Проверка ──────────────────────────────────────────────────────────────
echo ""
echo "==> Топики в кластере 2:"
docker exec kafka-analytics kafka-topics.sh \
  --list \
  --bootstrap-server kafka-analytics:9092 \
  --command-config /tmp/admin.properties

# Чистим временный файл
docker exec kafka-analytics rm -f /tmp/admin.properties

echo ""
echo "==> Кластер 2 настроен!"
echo ""
echo "Следующий шаг: запустить MirrorMaker"
echo "  cd ../mirrormaker && docker compose up -d"
echo ""
echo "После старта MirrorMaker топики cluster1.* появятся в кластере 2 автоматически."
