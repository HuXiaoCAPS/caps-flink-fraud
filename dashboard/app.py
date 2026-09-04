#!/usr/bin/env python3
"""Flask 后端：查询 Doris 风控告警结果，提供大屏 JSON API。

运行：
  .venv/bin/python dashboard/app.py
"""

from datetime import datetime, timedelta

import pymysql
from flask import Flask, jsonify, render_template

app = Flask(__name__)

DORIS = dict(host="localhost", port=9030, user="root", password="",
             database="risk", connect_timeout=5)

ALERTS_TABLE = "risk.dws_risk_result"


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
                 "city, risk_type, trigger_time, detail FROM " + ALERTS_TABLE
                 + " ORDER BY id DESC LIMIT 20")
    return jsonify(rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)