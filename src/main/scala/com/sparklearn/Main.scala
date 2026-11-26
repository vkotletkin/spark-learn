package com.sparklearn

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Entry point for local and cluster runs. Tiny Spark SQL sample.
 *
 * Master resolution (first match wins):
 *   1. CLI arg, e.g. spark://10.0.0.10:7077 or local[*]
 *   2. SPARK_MASTER env
 *   3. spark.master system property (set by spark-submit --master)
 *   4. local[*] (IDE / bare java -jar)
 */
object Main {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val master = args.headOption
      .orElse(sys.env.get("SPARK_MASTER"))
      .getOrElse(sys.props.getOrElse("spark.master", "local[*]"))

    val spark = SparkSession.builder()
      .appName("spark-learn")
      .master(master)
      // Kryo + Java 17/21: JVM must have --add-opens=java.base/java.nio=ALL-UNNAMED
      // BEFORE start. SparkSession.config is too late. Cluster mode: spark-submit
      // --conf spark.driver.extraJavaOptions / spark.executor.extraJavaOptions
      // (see installation/START.md). Client mode: spark-class already adds them.
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)
      spark.sql("SELECT 1 AS ok").show(false)
    } finally {
      spark.stop()
    }
  }
}
