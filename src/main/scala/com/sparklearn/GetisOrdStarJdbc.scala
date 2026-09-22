package com.sparklearn

import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.hotspotDetection.GetisOrd
import org.apache.spark.sql.functions.size
import org.slf4j.LoggerFactory

/**
 * Same Gi* as GetisOrdGiStar. The aggregate runs in ClickHouse via JDBC, so
 * from=/to= filter gprs_data there. Spark receives one row per base station.
 *
 *   from=2026-09-21 to=2026-09-22
 * Defaults to that day. BS_LOAD_FROM / BS_LOAD_TO override when args are absent.
 */
object GetisOrdStarJdbc {
  private val log = LoggerFactory.getLogger(getClass)
  private val Timestamp = """\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?""".r

  def main(args: Array[String]): Unit = {
    val spark = Sessions.sedonaWithClickHouse("spark-learn-gi-star-jdbc", args)
    import spark.implicits._

    val (from, to) = window(args)
    try {
      log.info("Spark {}, master={}, from={}, to={}", spark.version, spark.sparkContext.master, from, to)

      val host = sys.env.getOrElse("CLICKHOUSE_HOST", "localhost")
      val httpPort = sys.env.getOrElse("CLICKHOUSE_HTTP_PORT", "8123")
      val user = sys.env.getOrElse("CLICKHOUSE_USER", "spark")
      val password = sys.env.getOrElse("CLICKHOUSE_PASSWORD", "sparksecret")
      val database = sys.env.getOrElse("CLICKHOUSE_DATABASE", "spark")

      val query =
        s"""
           |SELECT
           |  b.id AS id,
           |  b.lat AS lat,
           |  b.lon AS lon,
           |  toFloat64(g.uniq_ident) AS uniq_ident
           |FROM $database.base_stations AS b
           |INNER JOIN
           |(
           |  SELECT
           |    code,
           |    lac,
           |    cellid,
           |    uniqExact(identificator) AS uniq_ident
           |  FROM $database.gprs_data
           |  WHERE timestamp >= '$from'
           |    AND timestamp < '$to'
           |  GROUP BY code, lac, cellid
           |) AS g
           |  ON b.code = g.code AND b.lac = g.lac AND b.cellid = g.cellid
         """.stripMargin

      spark.read
        .format("jdbc")
        .option("url", s"jdbc:clickhouse://$host:$httpPort/$database")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("user", user)
        .option("password", password)
        .option("dbtable", s"($query) AS ch_query")
        .load()
        .createOrReplaceTempView("bs_load")

      val stations = spark.sql(
        """
          |SELECT
          |  id,
          |  lat,
          |  lon,
          |  CAST(uniq_ident AS DOUBLE) AS uniq_ident,
          |  ST_Point(lon, lat) AS geometry
          |FROM bs_load
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

  private def window(args: Array[String]): (String, String) = {
    def one(name: String, env: String, default: String): String = {
      val raw = args.collectFirst { case a if a.startsWith(s"$name=") => a.drop(name.length + 1) }
        .orElse(sys.env.get(env))
        .getOrElse(default)
        .replace('T', ' ')
      if (!Timestamp.matches(raw)) {
        throw new IllegalArgumentException(s"$name must look like 2026-09-21 or 2026-09-21 00:00:00")
      }
      raw
    }
    (
      one("from", "BS_LOAD_FROM", "2026-09-21 00:00:00"),
      one("to", "BS_LOAD_TO", "2026-09-22 00:00:00")
    )
  }
}
