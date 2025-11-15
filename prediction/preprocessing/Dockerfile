FROM apache/spark:4.1.0-preview3-scala2.13-java21-python3-ubuntu
USER root

# install dependencies
RUN python3 -m pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir pandas numpy

# copy scripts
WORKDIR /app
COPY spark.py getSample.py ./

# prepare writable home for ivy / spark caches
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark
ENV HOME=/home/spark
ENV SPARK_USER_HOME=/home/spark
ENV IVY_HOME=/home/spark/.ivy2
RUN mkdir -p ${IVY_HOME}

USER spark

# default: export a snapshot once with kafka connectors
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview3,org.apache.kafka:kafka-clients:3.7.0", "/app/getSample.py", "--output", "../datasets/sample.json"]

