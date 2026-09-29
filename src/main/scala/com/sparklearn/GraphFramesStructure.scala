package com.sparklearn

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.slf4j.LoggerFactory

/**
 * GraphFrames: циклы и k-core.
 *
 *   mvn -Plocal scala:run -DmainClass=com.sparklearn.GraphFramesStructure
 *
 * Оба алгоритма чекпоинтят итерации. Локальный чекпоинт не требует отдельного
 * каталога на диске кластера.
 *
 * k-core считает неориентированный граф: обратное ребро (v, u) вместе с (u, v)
 * посчитается дважды, поэтому здесь каждое ребро задано один раз.
 */
object GraphFramesStructure {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = session(args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      // Два направленных цикла и мост vera -> gleb. Мост обратно не замыкается, цикла не даёт.
      val callers = Seq("anna", "boris", "vera", "gleb", "dina", "egor").toDF("id")
      val calls = Seq(
        ("anna", "boris"),
        ("boris", "vera"),
        ("vera", "anna"),
        ("gleb", "dina"),
        ("dina", "egor"),
        ("egor", "gleb"),
        ("vera", "gleb")
      ).toDF("src", "dst")

      println("\n=== cycles ===")
      GraphFrame(callers, calls).detectingCycles
        .setUseLocalCheckpoints(true)
        .run()
        .orderBy("id")
        .show(false)

      // Треугольник anna-boris-vera: у каждого степень 2, kcore = 2.
      // gleb висит на vera одним ребром, kcore = 1. egor ни с кем не связан, kcore = 0.
      val coreVertices = Seq("anna", "boris", "vera", "gleb", "egor").toDF("id")
      val coreEdges = Seq(
        ("anna", "boris"),
        ("boris", "vera"),
        ("vera", "anna"),
        ("vera", "gleb")
      ).toDF("src", "dst")

      println("\n=== k-core ===")
      GraphFrame(coreVertices, coreEdges).kCore
        .setUseLocalCheckpoints(true)
        .run()
        .select("id", "kcore")
        .orderBy("id")
        .show(false)
    } finally {
      spark.stop()
    }
  }

  private def session(args: Array[String]): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("spark-learn-graphframes-structure")
      .master(Sessions.master(args))
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
