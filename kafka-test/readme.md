
```bash
docker exec -it kafka1 bash
kafka-topics --create --topic test --bootstrap-server kafka1:9092
kafka-console-producer --topic test --bootstrap-server kafka1:9092
docker exec -it kafka2 bash
kafka-console-consumer --topic test --from-beginning --bootstrap-server kafka1:9092
```