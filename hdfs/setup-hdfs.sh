#!/bin/bash
# Czekaj aż NameNode wystartuje i będzie gotowy
echo "Waiting for NameNode to be ready..."
until docker exec namenode hdfs dfsadmin -report 2>/dev/null; do
    sleep 5
done

# Uruchom inicjalizację HDFS (tworzenie katalogów medallion)
docker exec namenode /opt/hadoop/init-hdfs.sh

echo "Bronze / Silver / Gold layers have been created in HDFS!"
