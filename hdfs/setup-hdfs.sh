#!/bin/bash
echo "Waiting for HDFS to be ready..."
sleep 10

NAMENODE_CONTAINER_ID=$(docker ps -q -f name=namenode)

if [ -z "$NAMENODE_CONTAINER_ID" ]; then
    echo "Namenode container not found."
    exit 1
fi

# Create base directories
docker exec "$NAMENODE_CONTAINER_ID" hdfs dfs -mkdir -p /bronze /silver /gold /test

# Create subdirectories for raw data
docker exec "$NAMENODE_CONTAINER_ID" hdfs dfs -mkdir -p /bronze/weather /bronze/traffic /bronze/air_pollution /bronze/uv

echo "HDFS directory structure created:"
docker exec "$NAMENODE_CONTAINER_ID" hdfs dfs -ls /
docker exec "$NAMENODE_CONTAINER_ID" hdfs dfs -ls /bronze