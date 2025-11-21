#!/bin/bash
# Wait for HDFS
until hdfs dfsadmin -report 2>/dev/null; do
    sleep 5
done

hdfs dfs -mkdir -p /bronze/raw
hdfs dfs -mkdir -p /silver/cleaned  
hdfs dfs -mkdir -p /gold/business
# Added permissive permissions for NiFi development writes
hdfs dfs -chmod -R 777 /bronze /silver /gold
echo "Medallion architecture created: /bronze /silver /gold"