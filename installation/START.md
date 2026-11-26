Start in cluster mode

Kryo on Java 17/21 needs `--add-opens` on the **driver JVM**. In `--deploy-mode cluster` Spark does not add them itself. Pass them at submit time, or put the same flags in `spark-defaults.conf` (see [CONF.md](CONF.md)).

```bash
ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

PACKAGES="org.apache.spark:spark-hadoop-cloud_2.13:4.2.1,com.clickhouse.spark:clickhouse-spark-runtime-4.2_2.13:0.11.0,com.clickhouse:clickhouse-jdbc:0.11.1:all,io.graphframes:graphframes-spark4_2.13:0.12.3"

bin/spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode cluster \
  --class com.sparklearn.Main \
  --driver-cores 2 \
  --driver-memory 2g \
  --executor-cores 2 \
  --executor-memory 2g \
  --packages "$PACKAGES" \
  --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
  --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
  /Users/vladislavkotletkin/development/spark-learn/spark-learn/target/spark-learn-1.0.0-uber.jar
```

`--packages` не нужен, если те же JAR-ы уже лежат в `$SPARK_HOME/jars` на всех узлах.

Скачать набор локально (версии из `pom.xml`):

```bash
./installation/fetch-spark-jars.sh
cp installation/spark-*-jars/*.jar "$SPARK_HOME/jars/"
```

Каталоги: `spark-cloud-jars/` (S3), `spark-clickhouse-jars/`, `spark-graphframes-jars/` (включая graphx + DataSketches).
