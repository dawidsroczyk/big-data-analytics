#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <topic>"
    exit 1
fi

TOPIC=$1
KAFKA_CONTAINER="kafka1"

echo "Consuming messages from topic '$TOPIC'..."
echo "Press Ctrl+C to stop."

docker exec "$KAFKA_CONTAINER" \
    kafka-console-consumer --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning
