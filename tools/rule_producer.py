#!/usr/bin/env python3
"""rule_producer.py - 规则更新生产者

向 Kafka `rule_topic` 发送规则更新指令，配合 Flink 作业的 Broadcast State
实现风控规则热更新（改规则、无需重启作业）。

用法示例：
  python3 tools/rule_producer.py                                  # 默认规则 R001 (threshold=900, 窗口30s)
  python3 tools/rule_producer.py --threshold 500 --window-ms 60000  # 热更新阈值/窗口
  python3 tools/rule_producer.py --name "大额交易(严)" --threshold 3000  # 换规则名+提高阈值
  python3 tools/rule_producer.py --disable                        # 停用规则
  python3 tools/rule_producer.py --rule-id R002 --threshold 800   # 新增一条规则
"""

import argparse
import json

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, RULE_TOPIC


def build_rule(args) -> dict:
    return {
        "rule_id": args.rule_id,
        "rule_name": args.name,
        "rule_type": args.rule_type,
        "threshold": args.threshold,
        "window_ms": args.window_ms,
        "enabled": not args.disable,
        "version": args.version,
        "weight": args.weight,
    }


def main():
    parser = argparse.ArgumentParser(description="风控规则更新生产者")
    parser.add_argument("--rule-id", default="R001")
    parser.add_argument("--name", default="连续大额交易")
    parser.add_argument("--rule-type", default="CONSECUTIVE_HIGH_AMOUNT")
    parser.add_argument("--threshold", type=float, default=900.0)
    parser.add_argument("--window-ms", type=int, default=30000)
    parser.add_argument("--disable", action="store_true", help="停用规则")
    parser.add_argument("--weight", type=float, default=0.5, help="规则权重(风险评分)")
    parser.add_argument("--version", type=int, default=2)
    args = parser.parse_args()

    rule = build_rule(args)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,
    )
    producer.send(RULE_TOPIC, key=rule["rule_id"], value=rule)
    producer.flush()
    producer.close(timeout=5)
    print(f"[ok] 规则已发送到 {RULE_TOPIC}: {rule}")


if __name__ == "__main__":
    main()