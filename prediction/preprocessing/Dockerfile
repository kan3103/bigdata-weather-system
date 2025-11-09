# weather_project/spark_app/Dockerfile

FROM apache/spark:4.1.0-preview3-scala2.13-java21-python3-ubuntu
USER root

# install dependencies
RUN python3 -m pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir pandas numpy

# copy script and data
WORKDIR /
COPY spark.py .
USER spark

# run spark-submit
CMD ["/opt/spark/bin/spark-submit", "/spark.py"]