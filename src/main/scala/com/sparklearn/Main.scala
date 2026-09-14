package com.sparklearn

import org.apache.spark.sql.SparkSession

/**
 * Entry point for local and cluster runs.
 *
 * Master resolution (first match wins):
 *   1. CLI arg, e.g. spark://10.0.0.10:7077 or local[*]
 *      2. SPARK_MASTER env
 *      3. spark.master system property (set by spark-submit --master)
 *      4. local[*] (IDE / bare java -jar
 */
object Main {
  def main(args: Array[String]): Unit = {

    val master = args.headOption
      .orElse(sys.env.get("SPARK_MASTER"))
      .getOrElse(sys.props.getOrElse("spark.master", "local[*]"))

    val spark = SparkSession.builder()
      .appName("spark-learn")
      .master(master)
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      println(s"Spark ${spark.version}")
      println(s"master=${sc.master}, appId=${sc.applicationId}")

      val count = spark.range(0, 100).count()
      println(s"range(0, 100).count() = $count")
    } finally {
      spark.stop()
    }
  }
}
