# Gold Features to MongoDB – Spark Streaming Job

## Uruchomienie joba `gold_features_to_mongo.py`

### 1) Skopiowanie pliku z hosta do kontenera `spark-master`

> Ustaw własną ścieżkę do pliku po swojej stronie (host).

```bash
docker cp /home/kubog/big-data-analytics/stream_preprocess/gold_features_to_mongo.py spark-master:/spark/jobs/gold_features_to_mongo.py
```

### 2) Sprawdzenie Spark UI PRZED uruchomieniem joba (WAŻNE)

Jeżeli wcześniej był uruchamiany jakikolwiek Spark Streaming job:

Otwórz Spark UI:

http://localhost:8080


Przejdź do zakładki Applications

Sprawdź, czy nie ma aplikacji w stanie WAITING lub aktywnego starego joba

Jeżeli są:

zatrzymaj poprzedni spark-submit

albo ubij job / zrestartuj kontenery Sparka

Przed uruchomieniem nowego joba NIE MOŻE działać żaden stary streaming job — inaczej będą konflikty i podwójne przetwarzanie.

### 3) Uruchomienie joba Spark Streaming

Uruchom job w kontenerze spark-master:

```bash
docker exec spark-master /spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
/spark/jobs/gold_features_to_mongo.py
```

Po uruchomieniu:

job powinien pojawić się w Spark UI jako RUNNING

w logach powinny pojawiać się wpisy [foreachBatch]

### 4) Weryfikacja zapisu danych w MongoDB

Wejście do mongosh:
```bash
docker exec -it mongo mongosh -u root -p rootpass --authenticationDatabase admin
````

Sprawdzenie danych:
```bash
use air_quality
show collections

db.gold_history.countDocuments()
db.gold_latest.countDocuments()

db.gold_history.find().sort({ event_ts_mongo: -1 }).limit(5).pretty()
db.gold_latest.find().limit(5).pretty()
```


Jeżeli:

countDocuments() zwraca wartości > 0

find() pokazuje dokumenty

→ job działa poprawnie i zapisuje dane.