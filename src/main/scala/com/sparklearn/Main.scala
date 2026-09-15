package com.sparklearn

import org.apache.sedona.spark.SedonaContext
import org.slf4j.LoggerFactory

/**
 * Entry point for local and cluster runs. Tiny Sedona SQL sample.
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

    val spark = SedonaContext.builder()
      .appName("spark-learn-sedona")
      .master(master)
      // Kryo + Java 17/21: JVM must have --add-opens=java.base/java.nio=ALL-UNNAMED
      // BEFORE start. SparkSession.config is too late. Cluster mode: spark-submit
      // --conf spark.driver.extraJavaOptions / spark.executor.extraJavaOptions
      // (see installation/START.md). Client mode: spark-class already adds them.
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .getOrCreate()

    val sedona = SedonaContext.create(spark)

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val points = sedona.sql(
        """
          |SELECT
          |  ST_Point(0.0, 0.0) AS a,
          |  ST_Point(3.0, 4.0) AS b
        """.stripMargin
      ).selectExpr(
        "a",
        "b",
        "ST_Distance(a, b) AS dist",
        "ST_Contains(ST_PolygonFromEnvelope(-1, -1, 1, 1), a) AS a_in_square"
      )

      points.show(false)
    } finally {
      spark.stop()
    }
  }
}
