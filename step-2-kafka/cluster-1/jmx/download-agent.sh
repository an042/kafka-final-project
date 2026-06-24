#!/bin/bash
# download-agent.sh — скачивает JMX Prometheus Exporter java-агент.
#
# JMX Exporter собирает метрики Kafka через JMX и экспортирует их в формате
# Prometheus на HTTP порту (9101/9102/9103 для каждого брокера).
#
# Запускать ПЕРЕД первым docker compose up кластера 1:
#   bash step-2-kafka/cluster-1/jmx/download-agent.sh

# Версия JMX Exporter (актуальна на момент создания проекта)
VERSION="1.0.1"

# Имя jar файла
JAR_NAME="jmx_prometheus_javaagent.jar"

# URL загрузки с GitHub Releases
URL="https://github.com/prometheus/jmx_exporter/releases/download/${VERSION}/jmx_prometheus_javaagent-${VERSION}.jar"

# Директория скрипта (jmx/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Скачиваем JMX Prometheus Exporter v${VERSION}..."
echo "    URL: ${URL}"
echo "    Сохраняем: ${SCRIPT_DIR}/${JAR_NAME}"

# curl -L — следовать редиректам (GitHub Releases использует redirect)
# curl -o — сохранить в файл с указанным именем
# --progress-bar — прогресс бар вместо подробного лога
curl -L --progress-bar -o "${SCRIPT_DIR}/${JAR_NAME}" "${URL}"

# Проверяем что файл скачался (не пустой)
if [ -s "${SCRIPT_DIR}/${JAR_NAME}" ]; then
  echo ""
  echo "    ОК: ${JAR_NAME} скачан ($(du -h "${SCRIPT_DIR}/${JAR_NAME}" | cut -f1))"
  echo ""
  echo "Следующие шаги:"
  echo "  1. cd step-2-kafka/cluster-1"
  echo "  2. docker compose up -d"
  echo "  3. bash setup.sh"
  echo "  4. cd ../../step-6-monitoring && docker compose up -d"
else
  echo "ОШИБКА: файл не скачался или пустой"
  echo "Попробуйте скачать вручную: ${URL}"
  exit 1
fi
