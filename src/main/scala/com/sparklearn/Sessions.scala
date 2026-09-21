package com.sparklearn

import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.SparkSession

/**
 * Shared local Spark + Sedona + ClickHouse catalog. Not an entry point.
 */
object Sessions {
  def master(args: Array[String]): String = {
    val fromArgs = args.find { a =>
      a == "local" ||
        a.startsWith("local[") ||
        a.startsWith("spark://") ||
        a.startsWith("yarn") ||
        a.startsWith("k8s://")
    }
    fromArgs
      .orElse(sys.env.get("SPARK_MASTER"))
      .getOrElse(sys.props.getOrElse("spark.master", "local[*]"))
  }

  def sedonaWithClickHouse(appName: String, args: Array[String]): SparkSession = {
    val host = sys.env.getOrElse("CLICKHOUSE_HOST", "localhost")
    val httpPort = sys.env.getOrElse("CLICKHOUSE_HTTP_PORT", "8123")
    val user = sys.env.getOrElse("CLICKHOUSE_USER", "spark")
    val password = sys.env.getOrElse("CLICKHOUSE_PASSWORD", "sparksecret")
    val database = sys.env.getOrElse("CLICKHOUSE_DATABASE", "spark")

    val spark = SedonaContext.builder()
      .appName(appName)
      .master(master(args))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
      .config("spark.sql.catalog.clickhouse.host", host)
      .config("spark.sql.catalog.clickhouse.protocol", "http")
      .config("spark.sql.catalog.clickhouse.http_port", httpPort)
      .config("spark.sql.catalog.clickhouse.user", user)
      .config("spark.sql.catalog.clickhouse.password", password)
      .config("spark.sql.catalog.clickhouse.database", database)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SedonaContext.create(spark)
  }
}
