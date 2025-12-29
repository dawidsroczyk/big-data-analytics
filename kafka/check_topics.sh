#!/bin/bash

# Script to check the status of Kafka topics

KAFKA_CONTAINER="kafka1"
TOPICS=("raw_weather" "raw_traffic" "raw_air_quality" "raw_uv")

echo "Checking Kafka topics status..."
echo "Bootstrap server is inside container $KAFKA_CONTAINER"
echo ""

for topic in "${TOPICS[@]}"; do
    echo "--- Describing topic: $topic ---"
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics --bootstrap-server localhost:9092 --describe --topic "$topic"
    echo ""
done

echo "Topic check complete."
