"""全局配置：Kafka / Doris 连接信息。"""

# ---- Kafka ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
ORDER_TOPIC = "order_topic"   # 交易数据流
RULE_TOPIC = "rule_topic"     # 规则更新流

# ---- Doris ----
DORIS_HOST = "localhost"
DORIS_QUERY_PORT = 9030        # MySQL 协议端口
DORIS_USER = "root"
DORIS_PASSWORD = ""
DORIS_DB = "risk"
DORIS_TABLE = "dws_risk_result"
DORIS_BE_NAME = "doris-be"
DORIS_BE_HEARTBEAT_PORT = 9050