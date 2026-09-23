package com.sparklearn

import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Local Outlier Factor on one subscriber's pings.
 *
 * k — сколько ближайших соседей участвуют в оценке.
 * lof около 1 — точка такая же плотная, как её соседи.
 * lof заметно больше 1 — точка стоит отдельно: соседи кучкуются, а она от них далеко.
 *
 * Дом — 12 пингов в десятках метров. Две точки в километрах от дома — выбросы.
 * useSphere — расстояние по сфере, в метрах.
 */
object Lof {
  private val log = LoggerFactory.getLogger(getClass)

  private val K = 5
  private val Home = (55.7000, 37.5000)

  def main(args: Array[String]): Unit = {
    val spark = session(args)
    import spark.implicits._

    spark.sparkContext.setCheckpointDir("target/lof-checkpoint")

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val pings = (
        blob(1, "home", Home, homeOffsets) ++
          Seq(
            ping(100, "stray", 55.7800, 37.7000),
            ping(101, "stray", 55.6200, 37.3000)
          )
      ).toDF("id", "kind", "lat", "lon")

      pings.createOrReplaceTempView("pings")
      val points = spark.sql(
        """
          |SELECT id, kind, lat, lon, ST_Point(lon, lat) AS geometry
          |FROM pings
        """.stripMargin
      )

      // Id вершины — хеш всей строки, полный дубль ломает KNN.
      // LOF чекпоинтит k-distance, без setCheckpointDir падает.
      val scored = LocalOutlierFactor.localOutlierFactor(
        points,
        K,
        "geometry",
        handleTies = false,
        useSphere = true
      ).cache()
      scored.count()

      log.info("LOF, higher first (k={})", K)
      scored
        .select($"id", $"kind", $"lat", $"lon", $"lof")
        .orderBy($"lof".desc)
        .show(20, truncate = false)
    } finally {
      spark.stop()
    }
  }

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

  private def ping(id: Int, kind: String, lat: Double, lon: Double): (Int, String, Double, Double) =
    (id, kind, lat, lon)

  // 4×3 с шагом 15 м и небольшим сдвигом, чтобы расстояния не совпали.
  // В куче больше k точек, поэтому соседи дома — другие пинги дома.
  private val homeOffsets: Seq[(Double, Double)] =
    for {
      east <- 0 until 4
      north <- 0 until 3
    } yield (east * 15.0 + north * 0.4, north * 15.0)

  private def shift(center: (Double, Double), eastM: Double, northM: Double): (Double, Double) = {
    val (lat0, lon0) = center
    val lat = lat0 + northM / 111_320.0
    val lon = lon0 + eastM / (111_320.0 * math.cos(math.toRadians(lat0)))
    (lat, lon)
  }

  private def session(args: Array[String]): SparkSession = {
    val spark = SedonaContext.builder()
      .appName("spark-learn-lof")
      .master(Sessions.master(args))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SedonaContext.create(spark)
  }
}
