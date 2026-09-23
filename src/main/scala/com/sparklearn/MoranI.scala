package com.sparklearn

import java.util.Locale

import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.autocorrelation.Moran
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.size
import org.slf4j.LoggerFactory

/**
 * Global Moran's I on a grid of cells.
 *
 * Соседи — соты ближе BandMeters. Себя в соседи не кладём.
 * I близко к 1 — похожая нагрузка лежит рядом.
 * I близко к 0 — нагрузка разбросана случайно.
 * I близко к -1 — рядом стоят непохожие значения.
 * Z — насколько I выше ожидания при случайном разбросе. P — вероятность такого Z.
 *
 * Западные две колонки загружены, восточные тихие. Шаг сетки 500 м, порог 700 м:
 * сосед по стороне входит, диагональ нет.
 */
object MoranI {
  private val log = LoggerFactory.getLogger(getClass)

  private val BandMeters = 700.0
  private val StepMeters = 500.0
  private val Origin = (55.7500, 37.6000)

  def main(args: Array[String]): Unit = {
    val spark = session(args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val cells = grid.toDF("id", "load", "lat", "lon")
      cells.createOrReplaceTempView("cells")
      val points = spark.sql(
        """
          |SELECT id, CAST(load AS DOUBLE) AS load, lat, lon, ST_Point(lon, lat) AS geometry
          |FROM cells
        """.stripMargin
      )

      // В соседе остаются id и load: Moran читает их из weights.
      val weighted = Weighting.addBinaryDistanceBandColumn(
        points,
        BandMeters,
        includeZeroDistanceNeighbors = false,
        includeSelf = false,
        geometry = "geometry",
        useSpheroid = true,
        savedAttributes = Seq("id", "load")
      )

      log.info("Cells and neighbor counts")
      weighted
        .select($"id", $"load", $"lat", $"lon", size($"weights").as("n_neighbors"))
        .orderBy($"id")
        .show(20, truncate = false)

      val moran = Moran.getGlobal(weighted, twoTailed = true, idColumn = "id", valueColumnName = "load")
      val summary = "Moran I=%s Z=%s P=%s".format(
        "%.3f".formatLocal(Locale.US, moran.getI),
        "%.3f".formatLocal(Locale.US, moran.getZNorm),
        "%.4f".formatLocal(Locale.US, moran.getPNorm)
      )
      log.info(summary)
      println(summary)
    } finally {
      spark.stop()
    }
  }

  /** 4×4. Колонки 0 и 1 — нагрузка 80, колонки 2 и 3 — нагрузка 10. */
  private def grid: Seq[(Int, Double, Double, Double)] =
    for {
      col <- 0 until 4
      row <- 0 until 4
    } yield {
      val (lat, lon) = shift(Origin, col * StepMeters, row * StepMeters)
      val load = if (col < 2) 80.0 else 10.0
      (row * 4 + col + 1, load, lat, lon)
    }

  private def shift(center: (Double, Double), eastM: Double, northM: Double): (Double, Double) = {
    val (lat0, lon0) = center
    val lat = lat0 + northM / 111_320.0
    val lon = lon0 + eastM / (111_320.0 * math.cos(math.toRadians(lat0)))
    (lat, lon)
  }

  private def session(args: Array[String]): SparkSession = {
    val spark = SedonaContext.builder()
      .appName("spark-learn-moran")
      .master(Sessions.master(args))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SedonaContext.create(spark)
  }
}
