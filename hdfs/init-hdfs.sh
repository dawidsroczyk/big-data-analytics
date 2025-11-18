#!/bin/bash
# Ten skrypt tworzy katalogi medallion w HDFS

set -e

# Utwórz katalogi w HDFS
hdfs dfs -mkdir -p /bronze/raw
hdfs dfs -mkdir -p /silver/cleaned
hdfs dfs -mkdir -p /gold/business

echo "Medallion architecture created successfully:"
echo "/bronze/raw"
echo "/silver/cleaned"
echo "/gold/business"
