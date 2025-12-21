FROM apache/hive:4.0.0

USER root

# Pobierz driver JDBC
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /tmp/postgresql.jar

# Wstaw do Hive + do Hadoop libs (schematool często to łapie stamtąd)
RUN cp /tmp/postgresql.jar /opt/hive/lib/postgresql.jar && \
    mkdir -p /opt/hadoop/share/hadoop/common/lib && \
    cp /tmp/postgresql.jar /opt/hadoop/share/hadoop/common/lib/postgresql.jar && \
    chmod 644 /opt/hive/lib/postgresql.jar /opt/hadoop/share/hadoop/common/lib/postgresql.jar

USER hive
