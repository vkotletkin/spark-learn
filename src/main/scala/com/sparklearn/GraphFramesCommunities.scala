package com.sparklearn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, sort_array}
import org.graphframes.GraphFrame
import org.slf4j.LoggerFactory

/**
 * GraphFrames на двух кругах абонентов и одном одностороннем мосте vera -> gleb.
 *
 *   mvn -Plocal scala:run -DmainClass=com.sparklearn.GraphFramesCommunities
 *
 * PageRank, компоненты и label propagation идут через GraphX: на Java 17/21
 * нужен --add-opens, как в installation/START.md.
 *
 * Круг anna-boris-vera и круг gleb-dina-egor — циклы. Мост склеивает их в одну
 * слабую компоненту, но обратно из второго круга в первый пройти нельзя, поэтому
 * сильно связных компонент две. PageRank выше у gleb: в него входит и свой круг, и мост.
 */
object GraphFramesCommunities {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = session(args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val vertices = Seq("anna", "boris", "vera", "gleb", "dina", "egor").toDF("id")
      val edges = Seq(
        ("anna", "boris", "circle"),
        ("boris", "vera", "circle"),
        ("vera", "anna", "circle"),
        ("gleb", "dina", "circle"),
        ("dina", "egor", "circle"),
        ("egor", "gleb", "circle"),
        ("vera", "gleb", "bridge")
      ).toDF("src", "dst", "kind")

      val calls = GraphFrame(vertices, edges)
      val circles = GraphFrame(vertices, edges.where("kind = 'circle'"))

      println("\n=== PageRank ===")
      calls.pageRank
        .resetProbability(0.15)
        .maxIter(10)
        .run()
        .vertices
        .select("id", "pagerank")
        .orderBy(col("pagerank").desc)
        .show(false)

      // Направление ребра не важно: мост склеивает оба круга в одну компоненту.
      println("\n=== connected components ===")
      members(
        calls.connectedComponents
          .setAlgorithm("graphx")
          .run()
      ).show(false)

      // Направление важно. Из второго круга в первый хода нет, мост компоненту не склеивает.
      println("\n=== strongly connected components ===")
      members(
        calls.stronglyConnectedComponents
          .maxIter(10)
          .run()
      ).show(false)

      // Без моста: у каждого абонента оба соседа внутри своего круга, метки не утекают.
      println("\n=== label propagation, circles only ===")
      members(
        circles.labelPropagation
          .setAlgorithm("graphx")
          .maxIter(5)
          .run()
      ).show(false)

      // Ребро приводится к src < dst, цикл из трёх становится треугольником. Мост треугольник не замыкает.
      println("\n=== triangle count ===")
      calls.triangleCount
        .setAlgorithm("exact")
        .run()
        .select("id", "count")
        .orderBy("id")
        .show(false)
    } finally {
      spark.stop()
    }
  }

  private def members(labeled: DataFrame): DataFrame = {
    val key = if (labeled.columns.contains("component")) "component" else "label"
    labeled
      .groupBy(key)
      .agg(sort_array(collect_list(col("id"))).as("members"))
      .orderBy(key)
  }

  private def session(args: Array[String]): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("spark-learn-graphframes-communities")
      .master(Sessions.master(args))
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("target/graphframes-checkpoint")
    spark
  }
}
