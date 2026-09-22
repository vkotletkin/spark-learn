package com.sparklearn

import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.hotspotDetection.GetisOrd
import org.apache.spark.sql.functions.size
import org.slf4j.LoggerFactory

/**
 * Getis-Ord Gi* on base-station centroids. x = uniq_ident from clickhouse.spark.bs_load.
 *
 * Neighbours = cells within 1.6 km (spheroid). includeSelf + star = Gi*, not Gi.
 *
 * Printed tables:
 *   orderBy uniq_ident → Luzhniki (one fat cluster)
 *   orderBy Z          → city center (high values vs the whole map)
 *   Losiny Ostrov      → negative Z (coldspot)
 */
object GetisOrdGiStar {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = Sessions.sedonaWithClickHouse("spark-learn-gi-star", args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val stations = spark.sql(
        """
          |SELECT
          |  id,
          |  lat,
          |  lon,
          |  CAST(uniq_ident AS DOUBLE) AS uniq_ident,
          |  ST_Point(lon, lat) AS geometry
          |FROM clickhouse.spark.bs_load
        """.stripMargin
      )

      val weighted = Weighting.addBinaryDistanceBandColumn(
        stations,
        100.0,
        includeZeroDistanceNeighbors = false,
        includeSelf = true,
        geometry = "geometry",
        useSpheroid = true,
        savedAttributes = Seq("id", "lat", "lon", "uniq_ident")
      )

      val giStar = GetisOrd.gLocal(weighted, "uniq_ident", star = true)
        .select(
          $"id",
          $"lat",
          $"lon",
          $"uniq_ident",
          size($"weights").as("n_neighbors"),
          $"G",
          $"EG",
          $"Z",
          $"P"
        )
        .cache()
      giStar.count()

      log.info("Top by uniq_ident (absolute load)")
      giStar.orderBy($"uniq_ident".desc).show(12, truncate = false)

      log.info("Top by Gi* Z (spatial hotspot vs the whole map)")
      giStar.orderBy($"Z".desc).show(12, truncate = false)

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

      log.info("Coldspots (lowest Z)")
      giStar.orderBy($"Z".asc).show(8, truncate = false)
    } finally {
      spark.stop()
    }
  }
}
