#!/bin/bash
until docker exec $(docker ps -q -f name=namenode) hdfs dfsadmin -report 2>/dev/null; do
    sleep 5
done

docker exec $(docker ps -q -f name=namenode) /opt/hadoop/init-hdfs.sh
echo "Bronze/silver/gold layers created"