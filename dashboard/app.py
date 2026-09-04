#!/usr/bin/env python3
"""Flask 后端：查询 Doris 风控告警结果，提供大屏 JSON API。

运行：
  .venv/bin/python dashboard/app.py
"""

from datetime import datetime, timedelta

import pymysql
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

DORIS = dict(host="localhost", port=9030, user="root", password="",
             database="risk", connect_timeout=5)

ALERTS_TABLE = "risk.dws_risk_result"

# ---- 规则管理（发送到 Kafka rule_topic，供 Flink 广播状态热更新） ----
KAFKA_BOOTSTRAP = "localhost:9092"
RULE_TOPIC = "rule_topic"
_rule_versions = {}


def fetch(sql, args=None):
    conn = pymysql.connect(**DORIS)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            cols = [d[0] for d in cur.description]
            rows = []
            for r in cur.fetchall():
                d = {}
                for k, v in zip(cols, r):
                    if isinstance(v, datetime):
                        d[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(v, (int, float)):
                        d[k] = v
                    else:
                        d[k] = str(v)
                rows.append(d)
            return rows
    finally:
        conn.close()


def scalar(sql, args=None, default=0):
    rows = fetch(sql, args)
    return rows[0] if rows else default


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    row = scalar("SELECT COUNT(*) total, COALESCE(SUM(total_amount), 0) amount "
                 "FROM " + ALERTS_TABLE)
    m5 = scalar("SELECT COUNT(*) c FROM " + ALERTS_TABLE
                + " WHERE trigger_time >= %s",
                (datetime.now() - timedelta(minutes=5),), {"c": 0})
    h1 = scalar("SELECT COUNT(*) c FROM " + ALERTS_TABLE
                + " WHERE trigger_time >= %s",
                (datetime.now() - timedelta(hours=1),), {"c": 0})
    rules = scalar("SELECT COUNT(DISTINCT rule_id) c FROM " + ALERTS_TABLE,
                   default={"c": 0})
    return jsonify({
        "total": row["total"],
        "total_amount": round(float(row["amount"]), 2),
        "last_5min": m5["c"],
        "last_1h": h1["c"],
        "rules": rules["c"],
        "updated_at": datetime.now().strftime("%H:%M:%S"),
    })


@app.route("/api/trend")
def api_trend():
    now = datetime.now()
    since = now - timedelta(minutes=30)
    rows = fetch("SELECT trigger_time FROM " + ALERTS_TABLE
                 + " WHERE trigger_time >= %s", (since,))
    counts = {}
    for r in rows:
        key = r["trigger_time"][11:16]  # "YYYY-MM-DD HH:MM:SS" -> "HH:MM"
        counts[key] = counts.get(key, 0) + 1
    labels, values = [], []
    t = since.replace(second=0, microsecond=0)
    while t <= now:
        key = t.strftime("%H:%M")
        labels.append(key)
        values.append(counts.get(key, 0))
        t += timedelta(minutes=1)
    return jsonify({"labels": labels, "values": values})


@app.route("/api/cities")
def api_cities():
    rows = fetch("SELECT city, COUNT(*) c FROM " + ALERTS_TABLE
                 + " GROUP BY city ORDER BY c DESC")
    return jsonify([{"name": r["city"], "value": r["c"]} for r in rows])


@app.route("/api/rules")
def api_rules():
    rows = fetch("SELECT rule_id, rule_name, COUNT(*) c FROM " + ALERTS_TABLE
                 + " GROUP BY rule_id, rule_name ORDER BY c DESC")
    # 同一 rule_id 可能存在历史规则名，取触发次数最多的名字展示
    best = {}
    for r in rows:
        rid = r["rule_id"]
        if rid not in best or r["c"] > best[rid]["count"]:
            best[rid] = {"rule_id": rid, "rule_name": r["rule_name"],
                         "count": r["c"]}
    return jsonify(list(best.values()))


@app.route("/api/latest")
def api_latest():
    rows = fetch("SELECT rule_id, rule_name, user_id, order_ids, total_amount, "
                 "city, risk_type, risk_score, trigger_time, detail FROM " + ALERTS_TABLE
                 + " ORDER BY trigger_time DESC LIMIT 20")
    return jsonify(rows)


def _latency_series(since):
    rows = fetch("SELECT window_end, trigger_time FROM " + ALERTS_TABLE
                 + " WHERE trigger_time >= %s", (since,))
    out = []
    for r in rows:
        wt = datetime.strptime(r["window_end"], "%Y-%m-%d %H:%M:%S")
        tt = datetime.strptime(r["trigger_time"], "%Y-%m-%d %H:%M:%S")
        lat = (tt - wt).total_seconds()
        if lat >= 0:
            out.append((tt, lat))
    return out


@app.route("/api/latency")
def api_latency():
    """端到端延迟：告警 trigger_time - 第二笔交易事件时间(window_end)。"""
    now = datetime.now()
    lats = [l for _, l in _latency_series(now - timedelta(hours=1))]
    stats = {}
    if lats:
        s = sorted(lats)
        n = len(s)
        stats = {"count": n, "mean": round(sum(lats) / n, 1),
                 "p50": round(s[int(n * 0.5)], 1), "p95": round(s[int(n * 0.95)], 1),
                 "p99": round(s[int(n * 0.99)], 1)}
    # 近 30 分钟每分钟平均延迟
    buckets = {}
    for tt, lat in _latency_series(now - timedelta(minutes=30)):
        buckets.setdefault(tt.strftime("%H:%M"), []).append(lat)
    labels, values = [], []
    t = (now - timedelta(minutes=30)).replace(second=0, microsecond=0)
    while t <= now:
        key = t.strftime("%H:%M")
        labels.append(key)
        arr = buckets.get(key, [])
        values.append(round(sum(arr) / len(arr), 1) if arr else 0)
        t += timedelta(minutes=1)
    return jsonify({"stats": stats, "labels": labels, "values": values})


@app.route("/api/rule", methods=["POST"])
def api_rule():
    """网页端规则热更新入口：将规则发送到 rule_topic。"""
    data = request.get_json(force=True) or {}
    rule_id = data.get("rule_id", "R001")
    version = _rule_versions.get(rule_id, 0) + 1
    _rule_versions[rule_id] = version
    rule = {
        "rule_id": rule_id,
        "rule_name": data.get("rule_name", "连续大额交易"),
        "rule_type": data.get("rule_type", "CONSECUTIVE_HIGH_AMOUNT"),
        "threshold": float(data.get("threshold", 900)),
        "window_ms": int(data.get("window_ms", 30000)),
        "enabled": bool(data.get("enabled", True)),
        "version": version,
        "weight": float(data.get("weight", 0.5)),
    }
    import json
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,
    )
    producer.send(RULE_TOPIC, key=rule["rule_id"], value=rule)
    producer.flush()
    producer.close(timeout=5)
    return jsonify({"ok": True, "rule": rule})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)