# SETUP
To do runowania jobów

`docker exec -it spark-master bash`

`/spark/bin/spark-submit --class preprocessing.job.GoldHiveJob --master spark://spark-master:7077 --executor-memory 4G --total-executor-cores 12 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`

`/spark/bin/spark-submit --class preprocessing.job.GoldHiveJob --master spark://spark-master:7077 --executor-memory 4G --total-executor-cores 12 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`

Jak bez setupów pamięci to można 

`/spark/bin/spark-submit --class preprocessing.job.FullPreproSafe --master spark://spark-master:7077 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`
`/spark/bin/spark-submit --class preprocessing.job.SilverHiveJob --master spark://spark-master:7077 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`
`/spark/bin/spark-submit --class preprocessing.job.GoldHiveJob --master spark://spark-master:7077 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`

## Hive interactive shell

`docker exec -it hive-server2 /bin/bash`

`!connect jdbc:hive2://localhost:10000`

Login, haslo hive hive

No i tu już SQL

`CREATE DATABASE IF NOT EXISTS silver;`
`CREATE DATABASE IF NOT EXISTS gold;`
