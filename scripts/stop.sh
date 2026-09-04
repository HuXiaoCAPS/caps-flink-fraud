#!/usr/bin/env bash
# 一键停止：模拟器 / 大屏 / Flink 作业 / 全部容器（数据卷保留）
set -uo pipefail
cd "$(dirname "$0")/.."

echo "[1/3] 停止模拟器与告警大屏..."
pkill -f "tools/mock_producer.py" 2>/dev/null || true
pkill -f "dashboard/app.py" 2>/dev/null || true

echo "[2/3] 移除 Flink 作业提交容器..."
docker rm -f fraud-job-cluster 2>/dev/null || true

echo "[3/3] 停止全部容器（Kafka / Doris / Flink）..."
docker compose down

echo "已停止。数据卷保留，重启用 ./scripts/start.sh"