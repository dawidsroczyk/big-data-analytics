# Kafka Setup

This directory contains the Docker Compose configuration for a multi-broker Kafka cluster running in KRaft mode. The setup includes two Kafka brokers and a setup service that creates predefined topics.

## Usage

### Prerequisites

- Docker and Docker Compose must be installed.
- The scripts should be executable:
  ```bash
  chmod +x check_topics.sh consume.sh
  ```

### Starting the Cluster

1.  Start the Kafka cluster in detached mode:
    ```bash
    docker-compose up -d
    ```
    This will start the Kafka brokers and create the following topics: `raw_weather`, `raw_traffic`, `raw_air_quality`, and `raw_uv`.

### Scripts

#### `check_topics.sh`

This dummy script checks the status and configuration of the predefined Kafka topics.

**Usage:**
```bash
./check_topics.sh
```

#### `consume.sh`

This script allows you to consume messages from a specified Kafka topic.

**Usage:**
```bash
./consume.sh <topic_name>
```

**Example:**
```bash
./consume.sh raw_weather
```
