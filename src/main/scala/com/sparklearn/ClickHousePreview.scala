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

      spark.sql("SELECT cgi, zone, expected_imsi, lon, lat FROM clickhouse.spark.cells ORDER BY cgi")
        .show(20, truncate = false)

      spark.sql(
        """
          |SELECT zone, count(*) AS cells, sum(expected_imsi) AS seeded_imsi
          |FROM clickhouse.spark.cells
          |GROUP BY zone
          |ORDER BY seeded_imsi DESC
        """.stripMargin
      ).show(truncate = false)

      spark.sql("SELECT count(*) AS positions FROM clickhouse.spark.positions").show()
      spark.sql("SELECT conn_type, count(*) AS n FROM clickhouse.spark.connections GROUP BY conn_type").show()
    } finally {
      spark.stop()
    }
  }
}
