#!/usr/bin/env python3
"""setup_doris.py - Doris 初始化脚本

Doris 2.1.11 镜像的入口脚本会自动完成 BE 注册（ALTER SYSTEM ADD BACKEND），
因此本脚本只需：
  1. 等待 FE 就绪（MySQL 协议端口可连）
  2. 等待 BE 状态变为 Alive
  3. 创建库表 risk.dws_risk_result

用法：
  python setup_doris.py
"""

import time

import pymysql

from config import (DORIS_DB, DORIS_HOST, DORIS_PASSWORD, DORIS_QUERY_PORT,
                    DORIS_TABLE, DORIS_USER)

DDL = f"""
CREATE TABLE IF NOT EXISTS `{DORIS_DB}`.`{DORIS_TABLE}` (
  `id`           BIGINT NOT NULL AUTO_INCREMENT,
  `rule_id`      VARCHAR(32),
  `rule_name`    VARCHAR(64),
  `user_id`      VARCHAR(32),
  `order_ids`    VARCHAR(255),
  `total_amount` DECIMAL(12, 2),
  `city`         VARCHAR(32),
  `risk_type`    VARCHAR(64),
  `window_start` DATETIME,
  `window_end`   DATETIME,
  `trigger_time` DATETIME,
  `detail`       VARCHAR(1024)
) UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
"""


def wait_fe(timeout: int = 180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(host=DORIS_HOST, port=DORIS_QUERY_PORT,
                                   user=DORIS_USER, password=DORIS_PASSWORD,
                                   connect_timeout=5)
            conn.close()
            print("[ok] FE ready")
            return
        except Exception:
            print(f"[.] waiting FE ... ({int(time.time() - start)}s)")
            time.sleep(5)
    raise SystemExit("FE 长时间未就绪，请检查 doris-fe 容器日志")


def get_backend_rows(conn: pymysql.Connection) -> tuple:
    cur = conn.cursor()
    cur.execute("SHOW BACKENDS")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return cols, rows


def wait_be_alive(conn: pymysql.Connection, timeout: int = 240):
    start = time.time()
    while time.time() - start < timeout:
        cols, rows = get_backend_rows(conn)
        alive_idx = cols.index("Alive")
        host_idx = cols.index("Host")
        if any(str(r[alive_idx]).lower() == "true" for r in rows):
            alive = [r[host_idx] for r in rows if str(r[alive_idx]).lower() == "true"]
            print(f"[ok] BE alive: {alive}")
            return
        print(f"[.] waiting BE alive ... ({int(time.time() - start)}s)")
        time.sleep(5)
    raise SystemExit("BE 未变为 Alive，请检查 doris-be 容器日志与 be.conf 的 priority_networks")


def init_schema(conn: pymysql.Connection):
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DORIS_DB}`")
    cur.execute(DDL)
    conn.commit()
    cur.close()
    print(f"[ok] schema ready: {DORIS_DB}.{DORIS_TABLE}")


def main():
    wait_fe()
    conn = pymysql.connect(host=DORIS_HOST, port=DORIS_QUERY_PORT,
                           user=DORIS_USER, password=DORIS_PASSWORD)
    try:
        wait_be_alive(conn)
        init_schema(conn)
    finally:
        conn.close()
    print("[done] Doris 就绪：FE(8030/9030) + BE(8040/9050)")


if __name__ == "__main__":
    main()