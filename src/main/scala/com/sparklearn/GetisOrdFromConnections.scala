package com.sparklearn

import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.hotspotDetection.GetisOrd
import org.apache.spark.sql.functions.size
import org.slf4j.LoggerFactory

/**
 * Same Gi* path after join: connection × position → CGI, then x = number of connections.
 *
 * Seed uses the same ts on both tables (equi-join). Production is as-of with TTL.
 */
object GetisOrdFromConnections {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = Sessions.sedonaWithClickHouse("spark-learn-gi-star-conn", args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val perCell = spark.sql(
        """
          |SELECT
          |  c.cgi,
          |  c.zone,
          |  CAST(count(*) AS DOUBLE) AS n_conn,
          |  ST_Point(c.lon, c.lat) AS geometry
          |FROM clickhouse.spark.connections conn
          |JOIN clickhouse.spark.positions p
          |  ON conn.imsi = p.imsi AND conn.ts = p.ts
          |JOIN clickhouse.spark.cells c
          |  ON p.cgi = c.cgi
          |GROUP BY c.cgi, c.zone, c.lon, c.lat
        """.stripMargin
      )

      val weighted = Weighting.addBinaryDistanceBandColumn(
        perCell,
        1600.0,
        includeZeroDistanceNeighbors = false,
        includeSelf = true,
        geometry = "geometry",
        useSpheroid = true,
        savedAttributes = Seq("cgi", "zone", "n_conn")
      )

      val giStar = GetisOrd.gLocal(weighted, "n_conn", star = true)
        .select(
          $"cgi",
          $"zone",
          $"n_conn",
          size($"weights").as("n_neighbors"),
          $"Z",
          $"P"
        )
        .cache()
      giStar.count()

      log.info("Top by connection count")
      giStar.orderBy($"n_conn".desc).show(10, truncate = false)

      log.info("Top by Gi* Z")
      giStar.orderBy($"Z".desc).show(10, truncate = false)

      log.info("Key zones")
      giStar
        .filter($"zone".isin("stadium", "downtown", "suburb_spike", "park"))
        .orderBy($"Z".desc)
        .show(20, truncate = false)
    } finally {
      spark.stop()
    }
  }
}
