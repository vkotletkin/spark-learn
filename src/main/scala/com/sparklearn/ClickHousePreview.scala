package com.sparklearn

import org.slf4j.LoggerFactory

/**
 * Reads demo tables from local ClickHouse (docker compose).
 *
 *   mvn -Plocal scala:run -DmainClass=com.sparklearn.ClickHousePreview
 *   spark-submit --class com.sparklearn.ClickHousePreview ...
 */
object ClickHousePreview {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = Sessions.sedonaWithClickHouse("spark-learn-ch-preview", args)
    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      spark.sql(
        "SELECT id, lat, lon, code, lac, cellid FROM clickhouse.spark.base_stations ORDER BY id"
      ).show(20, truncate = false)

      spark.sql(
        """
          |SELECT
          |  code,
          |  lac,
          |  cellid,
          |  count(*) AS gprs_rows,
          |  count(DISTINCT identificator) AS uniq_ident
          |FROM clickhouse.spark.gprs_data
          |GROUP BY code, lac, cellid
          |ORDER BY gprs_rows DESC
        """.stripMargin
      ).show(15, truncate = false)

      spark.sql("SELECT count(*) AS gprs_rows FROM clickhouse.spark.gprs_data").show()
    } finally {
      spark.stop()
    }
  }
}
