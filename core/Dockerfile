# Flink 作业容器镜像：与 Kafka / Doris 同网络运行（Stream Load 需直连 BE）
#
# 构建：docker build -f core/Dockerfile -t caps-fraud-job core/
# 运行：docker run -d --name fraud-job \
#           --network pyflink-cep-fraud-detection_default \
#           caps-fraud-job
FROM eclipse-temurin:8-jre

WORKDIR /app
COPY target/flink-fraud-detection-core-1.0.0.jar /app/app.jar

# 容器内通过服务名访问（compose 网络内互通）
ENV KAFKA_BOOTSTRAP=kafka:9094 \
    DORIS_FE_NODES=doris-fe:8030 \
    TZ=Asia/Shanghai

CMD ["java", "-Duser.timezone=Asia/Shanghai", "-jar", "/app/app.jar"]