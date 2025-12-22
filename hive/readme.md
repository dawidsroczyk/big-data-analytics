# SETUP
To do runowania jobów

`docker exec -it spark-master bash`

`/spark/bin/spark-submit --class preprocessing.job.GoldHiveJob --master spark://spark-master:7077 --executor-memory 4G --total-executor-cores 12 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`

`/spark/bin/spark-submit --class preprocessing.job.GoldHiveJob --master spark://spark-master:7077 --executor-memory 4G --total-executor-cores 12 /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`
