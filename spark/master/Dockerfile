FROM bde2020/spark-master:3.3.0-hadoop3.3

USER root

RUN apk update && \
    apk add --no-cache python3-dev py3-numpy


# Copy local Avro JAR
COPY ../spark-avro_2.12-3.3.0.jar /spark/jars/

# COPY jars/ /spark/jars/



