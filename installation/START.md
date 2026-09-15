Start in cluster mode

Kryo on Java 17/21 needs `--add-opens` on the **driver JVM**. In `--deploy-mode cluster` Spark does not add them itself. Pass them at submit time, or put the same flags in `spark-defaults.conf` (see [CONF.md](CONF.md)).

```bash
ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

bin/spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode cluster \
  --class com.sparklearn.Main \
  --driver-cores 2 \
  --driver-memory 2g \
  --executor-cores 2 \
  --executor-memory 2g \
  --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
  --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
  /Users/vladislavkotletkin/development/spark-learn/spark-learn/target/spark-learn-1.0.0-uber.jar
```
