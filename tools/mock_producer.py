#!/usr/bin/env python3
"""mock_producer.py - 实时交易数据模拟器

向 Kafka `order_topic` 生产模拟交易数据，包含三个核心特征：
  1. 异常注入：约 anomaly_rate（默认 4%）的交易为超大金额异常（>900 元）
  2. 乱序注入：约 delay_prob（默认 30%）的普通交易延迟 3~10 秒发送
  3. 爆发式异常：每 8~18 秒随机注入一名用户的 2~3 笔连续大额交易
     （模拟"同一用户短时间内连续大额交易"这种可被 CEP 捕获的模式）

用法：
  python mock_producer.py --rate 2 --anomaly-rate 0.04 --duration 0
"""

import argparse
import json
import random
import time
from collections import deque
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, ORDER_TOPIC

fake = Faker("zh_CN")

CITIES = ["上海", "北京", "深圳", "广州", "杭州", "成都", "武汉", "南京", "西安", "重庆"]
CHANNELS = ["app", "web", "pos", "h5"]
MERCHANTS = ["优衣库", "京东商城", "美团", "滴滴出行", "星巴克", "盒马鲜生", "永辉超市", "海底捞"]
CARD_BINS = ["6222", "6217", "6225", "4512", "4270"]

# 每个城市对应一段 IP 前缀，用于生成 ip 归属地异常的样本
CITY_IP_PREFIX = {
    "上海": "101.86", "北京": "111.200", "深圳": "113.108", "广州": "121.32",
    "杭州": "115.236", "成都": "125.70", "武汉": "113.57", "南京": "114.221",
    "西安": "117.32", "重庆": "113.248",
}

NORMAL_AMOUNT_RANGE = (10.0, 800.0)
ANOMALY_AMOUNT_RANGE = (900.0, 6000.0)


def random_ip(city: str) -> str:
    prefix = CITY_IP_PREFIX.get(city, "101.86")
    return f"{prefix}.{random.randint(1, 254)}.{random.randint(1, 254)}"


def format_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def gen_event(user_id: str, seq: int, ts_ms: int, city: str, amount: float, label: str) -> dict:
    return {
        "order_id": f"o_{user_id}_{seq}",
        "user_id": user_id,
        "amount": round(amount, 2),
        "ip": random_ip(city),
        "city": city,
        "channel": random.choice(CHANNELS),
        "merchant": random.choice(MERCHANTS),
        "card_bin": random.choice(CARD_BINS),
        "event_time": format_ts(ts_ms),
        "event_ts": ts_ms,
        "risk_label": label,
    }


class TransactionSimulator:
    """维护生成状态：延迟发送队列 + 各类统计。"""

    def __init__(self, anomaly_rate=0.04, delay_prob=0.30):
        self.anomaly_rate = anomaly_rate
        self.delay_prob = delay_prob
        self.pending = deque()          # (release_ts_seconds, event)
        self.seq = {}                   # user_id -> 订单序号
        self.stats = {"produced": 0, "delayed": 0, "anomaly": 0, "burst": 0}

    def next_seq(self, user_id: str) -> int:
        self.seq[user_id] = self.seq.get(user_id, 0) + 1
        return self.seq[user_id]

    def schedule_burst(self, now: float):
        """随机选一名用户，在未来 0.2~1.2 秒内连续发送 2~3 笔大额交易。"""
        user_id = "u_" + fake.uuid4()[:8]
        city = random.choice(CITIES)
        ts_ms = int(now * 1000)
        count = random.randint(2, 3)
        for i in range(count):
            ev = gen_event(user_id, self.next_seq(user_id), ts_ms, city,
                           random.uniform(*ANOMALY_AMOUNT_RANGE), "VELOCITY_BURST")
            self.pending.append((now + random.uniform(0.2, 1.2), ev))
            ts_ms += random.randint(300, 1200)
            self.stats["anomaly"] += 1
        self.stats["burst"] += 1

    def tick(self, now: float) -> list:
        """每个 tick 生成的事件（含已到期的延迟事件）。"""
        out = []
        # 1) 释放延迟队列中已到期的事件
        while self.pending and self.pending[0][0] <= now:
            _, ev = self.pending.popleft()
            out.append(ev)

        # 2) 生成一条新交易
        user_id = "u_" + fake.uuid4()[:8]
        city = random.choice(CITIES)
        ts_ms = int(now * 1000)

        if random.random() < self.anomaly_rate:
            ev = gen_event(user_id, self.next_seq(user_id), ts_ms, city,
                           random.uniform(*ANOMALY_AMOUNT_RANGE), "HIGH_AMOUNT")
            self.stats["anomaly"] += 1
            out.append(ev)  # 异常事件即时发送，保证 CEP 能及时捕获
        else:
            ev = gen_event(user_id, self.next_seq(user_id), ts_ms, city,
                           random.uniform(*NORMAL_AMOUNT_RANGE), "NORMAL")
            if random.random() < self.delay_prob:
                # 乱序注入：event_time 不变，但延迟 3~10 秒再发送
                self.pending.append((now + random.uniform(3.0, 10.0), ev))
                self.stats["delayed"] += 1
            else:
                out.append(ev)

        self.stats["produced"] += 1
        return out


def main():
    parser = argparse.ArgumentParser(description="实时交易数据模拟器")
    parser.add_argument("--rate", type=float, default=2.0, help="每秒产生事件数 (default: 2)")
    parser.add_argument("--anomaly-rate", type=float, default=0.04, help="异常注入比例 (default: 0.04)")
    parser.add_argument("--delay-prob", type=float, default=0.30, help="乱序注入比例 (default: 0.30)")
    parser.add_argument("--burst-interval", type=str, default="8-18",
                        help="爆发间隔秒数区间 min-max (default: 8-18，如 2-5 则更密)")
    parser.add_argument("--duration", type=float, default=0, help="运行秒数，0 表示一直运行")
    args = parser.parse_args()

    try:
        burst_lo, burst_hi = (float(x) for x in args.burst_interval.split("-"))
    except ValueError:
        parser.error("--burst-interval 需为 min-max 格式，如 2-5")
    burst_lo, burst_hi = max(burst_lo, 0.2), max(burst_hi, burst_lo)

    print(f"[init] Kafka={KAFKA_BOOTSTRAP_SERVERS} topic={ORDER_TOPIC} "
          f"rate={args.rate}/s anomaly={args.anomaly_rate:.0%} delay={args.delay_prob:.0%} "
          f"burst={burst_lo}-{burst_hi}s")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,
        linger_ms=5,
        retries=5,
        compression_type="gzip",
    )

    sim = TransactionSimulator(anomaly_rate=args.anomaly_rate, delay_prob=args.delay_prob)
    interval = 1.0 / max(args.rate, 0.1)
    next_burst_at = time.time() + random.uniform(burst_lo, burst_hi)
    last_log = time.time()
    start = time.time()

    try:
        while True:
            now = time.time()
            for ev in sim.tick(now):
                producer.send(ORDER_TOPIC, key=ev["user_id"], value=ev)

            if now >= next_burst_at:
                sim.schedule_burst(now)
                next_burst_at = now + random.uniform(burst_lo, burst_hi)
                producer.flush()

            if now - last_log >= 10:
                elapsed = now - start
                s = sim.stats
                print(f"[stats] {elapsed:.0f}s | produced={s['produced']} "
                      f"delayed={s['delayed']} anomaly={s['anomaly']} bursts={s['burst']} "
                      f"pending={len(sim.pending)}")
                last_log = now

            if args.duration and (time.time() - start) >= args.duration:
                print(f"[done] 达到 duration={args.duration}s，退出。共生产 {sim.stats['produced']} 条")
                break

            producer.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[exit] 用户中断")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()