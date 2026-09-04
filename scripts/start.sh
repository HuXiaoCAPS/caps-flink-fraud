#!/usr/bin/env bash
# 一键启动：Kafka + Doris + Flink Standalone 集群 + 作业 + 模拟器 + 告警大屏
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

# 0. 确保 flink 镜像存在（清理后可自动从镜像源拉取）
if ! docker image inspect flink:1.20.0 >/dev/null 2>&1; then
  echo "[0/6] 拉取 flink:1.20.0 镜像..."
  docker pull docker.m.daocloud.io/library/flink:1.20.0
  docker tag docker.m.daocloud.io/library/flink:1.20.0 flink:1.20.0
fi

echo "[1/6] 启动 Kafka / Doris / Flink 集群..."
docker compose up -d

echo "[2/6] 等待 Doris 就绪并初始化表结构..."
.venv/bin/python tools/setup_doris.py

echo "[3/6] 等待 Flink WebUI 就绪..."
for i in $(seq 1 30); do
  if curl -s -o /dev/null --max-time 3 http://localhost:8081/overview; then
    echo "  Flink WebUI 就绪 (${i}s)"
    break
  fi
  [ "$i" = 30 ] && { echo "  Flink 未就绪，请检查 flink-jobmanager 日志"; exit 1; }
  sleep 2
done

echo "[4/6] 构建作业 jar 并提交到 Flink 集群..."
mvn -q -f core/pom.xml package -DskipTests
docker rm -f fraud-job-cluster 2>/dev/null || true
docker run -d --name fraud-job-cluster --network pyflink-cep-fraud-detection_default \
  -v "$(pwd)/core/target:/app" \
  -e KAFKA_BOOTSTRAP=kafka:9094 -e DORIS_FE_NODES=doris-fe:8030 \
  flink:1.20.0 \
  /opt/flink/bin/flink run -d -m flink-jobmanager:8081 \
    -c com.fraud.FraudDetectionJob /app/flink-fraud-detection-core-1.0.0.jar

echo "[5/6] 下发初始规则 (R001~R007)..."
.venv/bin/python tools/rule_producer.py --rule-id R001 --name "连续大额交易" --rule-type CONSECUTIVE_HIGH_AMOUNT --threshold 900 --weight 0.6
.venv/bin/python tools/rule_producer.py --rule-id R002 --name "单笔大额交易" --rule-type SINGLE_HIGH_AMOUNT --threshold 2000 --weight 0.4
.venv/bin/python tools/rule_producer.py --rule-id R003 --name "连续超高额交易" --rule-type CONSECUTIVE_HIGH_AMOUNT --threshold 4000 --weight 0.8
.venv/bin/python tools/rule_producer.py --rule-id R004 --name "高频交易" --rule-type HIGH_FREQUENCY --threshold 0 --window-ms 30000 --weight 0.7 --count 3
.venv/bin/python tools/rule_producer.py --rule-id R005 --name "异地IP切换" --rule-type IP_CHANGE --threshold 0 --window-ms 60000 --weight 0.5
.venv/bin/python tools/rule_producer.py --rule-id R006 --name "小额试探后大额" --rule-type SEQUENCE_SMALL_LARGE --threshold 1500 --small-threshold 300 --weight 0.6
.venv/bin/python tools/rule_producer.py --rule-id R007 --name "大额未完(超时)" --rule-type TIMEOUT_ALERT --threshold 3000 --window-ms 120000 --weight 0.5

echo "[6/6] 启动数据模拟器与告警大屏..."
setsid .venv/bin/python tools/mock_producer.py --rate 3 > logs/producer.log 2>&1 &
setsid .venv/bin/python dashboard/app.py > logs/dashboard.log 2>&1 &

echo ""
echo "启动完成！"
echo "  Flink WebUI : http://localhost:8081"
echo "  告警大屏    : http://localhost:5001"
echo "  停止: ./scripts/stop.sh"