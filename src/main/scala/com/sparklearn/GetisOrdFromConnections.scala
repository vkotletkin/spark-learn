package com.sparklearn

import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.hotspotDetection.GetisOrd
import org.apache.spark.sql.functions.size
import org.slf4j.LoggerFactory

/**
 * Same Gi* path on clickhouse.spark.bs_load. x = n_gprs.
 */
object GetisOrdFromConnections {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = Sessions.sedonaWithClickHouse("spark-learn-gi-star-conn", args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val perStation = spark.sql(
        """
          |SELECT
          |  id,
          |  lat,
          |  lon,
          |  CAST(n_gprs AS DOUBLE) AS n_gprs,
          |  ST_Point(lon, lat) AS geometry
          |FROM clickhouse.spark.bs_load
        """.stripMargin
      )

      val weighted = Weighting.addBinaryDistanceBandColumn(
        perStation,
        1600.0,
        includeZeroDistanceNeighbors = false,
        includeSelf = true,
        geometry = "geometry",
        useSpheroid = true,
        savedAttributes = Seq("id", "lat", "lon", "n_gprs")
      )

      val giStar = GetisOrd.gLocal(weighted, "n_gprs", star = true)
        .select(
          $"id",
          $"lat",
          $"lon",
          $"n_gprs",
          size($"weights").as("n_neighbors"),
          $"Z",
          $"P"
        )
        .cache()
      giStar.count()

      log.info("Top by GPRS row count")
      giStar.orderBy($"n_gprs".desc).show(10, truncate = false)

      log.info("Top by Gi* Z")
      giStar.orderBy($"Z".desc).show(10, truncate = false)

      log.info("Key stations (Luzhniki, center, Solntsevo, Losiny Ostrov)")
      giStar
        .filter(
          ($"lat".between(55.710, 55.722) && $"lon".between(37.545, 37.565)) ||
            ($"lat".between(55.740, 55.770) && $"lon".between(37.590, 37.650)) ||
            ($"lat".between(55.574, 55.586) && $"lon".between(37.462, 37.478)) ||
            ($"lat".between(55.855, 55.885) && $"lon".between(37.755, 37.805))
        )
        .orderBy($"Z".desc)
        .show(20, truncate = false)
    } finally {
      spark.stop()
    }
  }
}
