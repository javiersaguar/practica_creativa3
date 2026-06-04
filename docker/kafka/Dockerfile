FROM ubuntu:22.04

RUN apt-get update && apt-get install -y wget openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV KAFKA_VERSION=4.2.0
ENV KAFKA_HOME=/opt/kafka

RUN wget -q https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_2.13-${KAFKA_VERSION}.tgz -C /opt && \
    mv /opt/kafka_2.13-${KAFKA_VERSION} ${KAFKA_HOME} && \
    rm kafka_2.13-${KAFKA_VERSION}.tgz

COPY start-kafka.sh /start-kafka.sh
RUN chmod +x /start-kafka.sh

EXPOSE 9092 9093

CMD ["/start-kafka.sh"]
