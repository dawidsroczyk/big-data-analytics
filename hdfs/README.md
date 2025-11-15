# HDFS — Setup

Run docker-compose and initialize HDFS:
```bash
docker compose up -d
./setup-hdfs.sh
```

Connect to NameNode:
```bash
./nn-bash.sh

# Example inside NameNode:
hdfs dfs -ls /bronze
```

Web UIs (default):
- NameNode: http://localhost:9870
- ResourceManager (YARN): http://localhost:8088

