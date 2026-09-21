package com.sparklearn

import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.hotspotDetection.GetisOrd
import org.apache.spark.sql.functions.size
import org.slf4j.LoggerFactory

/**
 * Getis-Ord Gi* on cell centroids. x = uniq IMSI from positions.
 *
 * Neighbours = cells within 1.6 km (spheroid). includeSelf + star = Gi*, not Gi.
 *
 * Printed tables:
 *   orderBy uniq_imsi → stadium (one fat cell)
 *   orderBy Z         → downtown (cluster of high values vs the whole map)
 *   park / quiet      → negative Z (coldspot)
 */
object GetisOrdGiStar {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = Sessions.sedonaWithClickHouse("spark-learn-gi-star", args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val cells = spark.sql(
        """
          |SELECT
          |  c.cgi,
          |  c.zone,
          |  CAST(count(DISTINCT p.imsi) AS DOUBLE) AS uniq_imsi,
          |  ST_Point(c.lon, c.lat) AS geometry
          |FROM clickhouse.spark.cells c
          |JOIN clickhouse.spark.positions p ON p.cgi = c.cgi
          |GROUP BY c.cgi, c.zone, c.lon, c.lat
        """.stripMargin
      )

      val weighted = Weighting.addBinaryDistanceBandColumn(
        cells,
        1600.0,
        includeZeroDistanceNeighbors = false,
        includeSelf = true,
        geometry = "geometry",
        useSpheroid = true,
        savedAttributes = Seq("cgi", "zone", "uniq_imsi")
      )

      val giStar = GetisOrd.gLocal(weighted, "uniq_imsi", star = true)
        .select(
          $"cgi",
          $"zone",
          $"uniq_imsi",
          size($"weights").as("n_neighbors"),
          $"G",
          $"EG",
          $"Z",
          $"P"
        )
        .cache()
      giStar.count()

      log.info("Top by uniq_imsi (absolute load)")
      giStar.orderBy($"uniq_imsi".desc).show(12, truncate = false)

      log.info("Top by Gi* Z (spatial hotspot vs the whole map)")
      giStar.orderBy($"Z".desc).show(12, truncate = false)

      log.info("Key zones")
      giStar
        .filter($"zone".isin("stadium", "downtown", "suburb_spike", "park"))
        .orderBy($"Z".desc)
        .show(20, truncate = false)

      log.info("Coldspots (lowest Z)")
      giStar.orderBy($"Z".asc).show(8, truncate = false)
    } finally {
      spark.stop()
    }
  }
}
