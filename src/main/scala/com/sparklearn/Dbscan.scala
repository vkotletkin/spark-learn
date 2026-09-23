package com.sparklearn

import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.stats.clustering.DBSCAN
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, collect_set, concat_ws, count, round}
import org.slf4j.LoggerFactory

/**
 * DBSCAN on one subscriber's pings.
 *
 * epsilon — насколько далеко другой пинг ещё считается соседом, метры (useSpheroid).
 * minPts  — сколько точек в этом круге нужно, чтобы точка стала ядром.
 * Ядро тянет соседей, цепочка ядер склеивает кластер.
 * Точка, до которой не дотянулось ни одно ядро, — шум, cluster = -1.
 * Остальные id — метки компонент графа соседей, не порядковые 0, 1, 2.
 *
 * Дом и работа — пинги в десятках метров друг от друга. Дорога — редкие точки
 * дальше epsilon, они остаются шумом.
 */
object Dbscan {
  private val log = LoggerFactory.getLogger(getClass)

  private val EpsilonMeters = 250.0
  private val MinPts = 20

  private val Home = (55.7000, 37.5000)
  private val Work = (55.7550, 37.6200)

  def main(args: Array[String]): Unit = {
    val spark = session(args)
    import spark.implicits._

    spark.sparkContext.setCheckpointDir("target/dbscan-checkpoint")

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val pings = (
        blob(1, "home", Home, blobOffsets) ++
          blob(100, "work", Work, blobOffsets) ++
          road(200, Home, Work, steps = 5)
      ).toDF("id", "kind", "lat", "lon")

      pings.createOrReplaceTempView("pings")
      val points = spark.sql(
        """
          |SELECT id, kind, lat, lon, ST_Point(lon, lat) AS geometry
          |FROM pings
        """.stripMargin
      )

      // Id вершины — хеш всей строки. Одинаковые координаты в разное время оставляем:
      // расстояние 0, они и набирают minPts. Полный дубль строки даёт один хеш и ломает граф.
      val clustered = DBSCAN.dbscan(
        points,
        EpsilonMeters,
        MinPts,
        "geometry",
        includeOutliers = true,
        useSpheroid = true
      ).cache()
      clustered.count()

      log.info("Clusters ( -1 = noise )")
      clustered
        .groupBy($"cluster")
        .agg(
          count("*").as("n"),
          round(avg($"lat"), 5).as("lat"),
          round(avg($"lon"), 5).as("lon"),
          concat_ws(",", collect_set($"kind")).as("kind")
        )
        .orderBy($"cluster")
        .show(truncate = false)

      log.info("Points")
      clustered
        .select($"id", $"kind", $"lat", $"lon", $"isCore", $"cluster")
        .orderBy($"cluster", $"id")
        .show(60, truncate = false)
    } finally {
      spark.stop()
    }
  }

  /** Пинги в радиусе десятков метров от центра. Вместе они плотнее minPts. */
  private def blob(
      id0: Int,
      kind: String,
      center: (Double, Double),
      offsets: Seq[(Double, Double)]
  ): Seq[(Int, String, Double, Double)] =
    offsets.zipWithIndex.map { case ((eastM, northM), i) =>
      val (lat, lon) = shift(center, eastM, northM)
      (id0 + i, kind, lat, lon)
    }

  /** Точки вдоль отрезка. Шаг больше epsilon, поэтому дорога не склеивается в кластер. */
  private def road(
      id0: Int,
      from: (Double, Double),
      to: (Double, Double),
      steps: Int
  ): Seq[(Int, String, Double, Double)] =
    (1 to steps).map { i =>
      val t = i.toDouble / (steps + 1)
      val lat = from._1 + (to._1 - from._1) * t
      val lon = from._2 + (to._2 - from._2) * t
      (id0 + i, "road", lat, lon)
    }

  // 5×4 пинга с шагом 20 м. Диаметр кучи ~100 м, все попадают в круг 250 м и набирают minPts.
  private val blobOffsets: Seq[(Double, Double)] =
    for {
      east <- Seq(-40.0, -20.0, 0.0, 20.0, 40.0)
      north <- Seq(-30.0, -10.0, 10.0, 30.0)
    } yield (east, north)

  private def shift(center: (Double, Double), eastM: Double, northM: Double): (Double, Double) = {
    val (lat0, lon0) = center
    val lat = lat0 + northM / 111_320.0
    val lon = lon0 + eastM / (111_320.0 * math.cos(math.toRadians(lat0)))
    (lat, lon)
  }

  private def session(args: Array[String]): SparkSession = {
    val spark = SedonaContext.builder()
      .appName("spark-learn-dbscan")
      .master(Sessions.master(args))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SedonaContext.create(spark)
  }
}
