package com.sparklearn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame
import org.slf4j.LoggerFactory

/**
 * GraphFrames на звонках: степени, кратчайший путь, расстояния до ориентира, взаимные звонки.
 *
 *   mvn -Plocal scala:run -DmainClass=com.sparklearn.GraphFramesPaths
 *
 * shortestPaths идёт через GraphX: на Java 17/21 нужен --add-opens, как в installation/START.md.
 *
 * anna и boris звонят друг другу. До gleb от anna короче через vera.
 * dina — ориентир: до неё ведут gleb и egor.
 */
object GraphFramesPaths {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = session(args)
    import spark.implicits._

    try {
      log.info("Spark {}, master={}", spark.version, spark.sparkContext.master)

      val vertices = Seq("anna", "boris", "vera", "gleb", "dina", "egor")
        .toDF("id")
      val edges = Seq(
        ("anna", "boris"),
        ("boris", "anna"),
        ("boris", "vera"),
        ("anna", "vera"),
        ("vera", "gleb"),
        ("gleb", "dina"),
        ("egor", "dina")
      ).toDF("src", "dst")

      val calls = GraphFrame(vertices, edges)

      println("\n=== degrees / in / out ===")
      calls.degrees.orderBy("id").show(false)
      calls.inDegrees.orderBy("id").show(false)
      calls.outDegrees.orderBy("id").show(false)

      // Только кратчайшие. anna -> vera -> gleb, два ребра. Путь через boris длиннее и не попадёт.
      println("\n=== BFS anna -> gleb ===")
      calls.bfs
        .fromExpr("id = 'anna'")
        .toExpr("id = 'gleb'")
        .maxPathLength(5)
        .run()
        .select(
          col("from.id").as("from"),
          col("v1.id").as("via"),
          col("to.id").as("to")
        )
        .show(false)

      // distances — map: id ориентира -> число рёбер. Кого не достать, в map нет.
      println("\n=== shortest paths to dina ===")
      calls.shortestPaths
        .landmarks(Seq("dina"))
        .setAlgorithm("graphx")
        .run()
        .orderBy("id")
        .show(false)

      // Мотив «позвонили друг другу». a.id < b.id убирает пару (boris, anna) как дубль (anna, boris).
      println("\n=== mutual calls ===")
      calls
        .find("(a)-[]->(b); (b)-[]->(a)")
        .filter("a.id < b.id")
        .select(col("a.id").as("a"), col("b.id").as("b"))
        .show(false)
    } finally {
      spark.stop()
    }
  }

  private def session(args: Array[String]): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("spark-learn-graphframes-paths")
      .master(Sessions.master(args))
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("target/graphframes-checkpoint")
    spark
  }
}
