#!/usr/bin/env python3
"""simple_consumer.py - 连通性测试消费端

消费 `order_topic`，打印交易字段与"到达延迟"，用于验证：
  1. Kafka 链路通畅
  2. 存在乱序数据（arrival_delay 为正的延迟到达事件）
  3. 存在异常数据（risk_label != NORMAL）

用法：
  python simple_consumer.py --count 20 --offset latest
"""

import argparse
import json
import time

from kafka import KafkaConsumer

from config import KAFKA_BOOTSTRAP_SERVERS, ORDER_TOPIC


def main():
    parser = argparse.ArgumentParser(description="连通性测试消费端")
    parser.add_argument("--count", type=int, default=20, help="消费条数")
    parser.add_argument("--offset", choices=["latest", "earliest"], default="latest")
    parser.add_argument("--group", default="conn-test")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset=args.offset,
        enable_auto_commit=False,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=15000,
    )

    print(f"[init] 订阅 {ORDER_TOPIC} (offset={args.offset})，最多消费 {args.count} 条\n")
    print(f"{'event_time':<26} {'user':<12} {'amount':>9} {'city':<6} "
          f"{'label':<15} {'arrival_delay(s)':>15}")
    print("-" * 90)

    seen = 0
    for msg in consumer:
        ev = msg.value
        arrival_ms = int(time.time() * 1000)
        delay_s = (arrival_ms - ev["event_ts"]) / 1000.0
        print(f"{ev['event_time']:<26} {ev['user_id']:<12} {ev['amount']:>9.2f} "
              f"{ev['city']:<6} {ev['risk_label']:<15} {delay_s:>15.1f}")
        seen += 1
        if seen >= args.count:
            break

    print(f"\n[done] 共消费 {seen} 条")


if __name__ == "__main__":
    main()