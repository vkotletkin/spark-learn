Start in cluster mode

```bash
spark-4.2.0-bin-hadoop3 % bin/spark-submit \
--master spark://localhost:7077 \
--deploy-mode cluster \
--supervise \
--class com.sparklearn.Main \
--driver-cores 2 \
--driver-memory 2g \
--executor-cores 2 \
--executor-memory 2g \
/Users/vladislavkotletkin/development/spark-learn/spark-learn/target/spark-learn-1.0.0-uber.jar```bash
```

```
spark-submit --master spark://localhost:7077 --deploy-mode cluster --clas com.sparklearn.Main /Users/vladislavkotletkin/development
/spark-learn/spark-learn/target/spark-learn-1.0.0-uber.jar
```