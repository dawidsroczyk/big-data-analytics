#!/bin/bash
# Wait for HDFS
until hdfs dfsadmin -report 2>/dev/null; do
    sleep 5
done

hdfs dfs -mkdir -p /bronze/raw
hdfs dfs -mkdir -p /silver/cleaned  
hdfs dfs -mkdir -p /gold/business

echo "Medallion architecture created: /bronze /silver /gold"