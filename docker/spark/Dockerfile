FROM spark:4.1.1-scala2.13-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir \
    pymongo \
    kafka-python \
    cassandra-driver \
    minio \
    findspark \
    iso8601 \
    numpy \
    scipy \
    scikit-learn \
    mlflow
RUN mkdir -p /home/spark/.ivy2/cache && \
    mkdir -p /home/spark/.ivy2/jars && \
    mkdir -p /tmp/.ivy2/cache && \
    mkdir -p /tmp/.ivy2/jars && \
    mkdir -p /app/jars && \
    mkdir -p /app/models && \
    chmod -R 777 /tmp/.ivy2 && \
    chmod -R 777 /app && \
    chown -R spark:spark /home/spark
ENV HOME=/home/spark
COPY download_jars.sh /download_jars.sh
RUN chmod +x /download_jars.sh && /download_jars.sh
COPY flight_prediction_2.13-0.1.jar /app/jars/flight_prediction_2.13-0.1.jar
COPY models/ /app/models/
RUN cp /home/spark/.ivy2/jars/*.jar /opt/spark/jars/
USER spark
