package com.sparklearn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

/**
 * Entry point for local and cluster runs. Writes a small DataFrame to S3
 * (SeaweedFS by default) and reads it back.
 *
 * Master resolution (first match wins):
 *   1. CLI arg, e.g. spark://10.0.0.10:7077 or local[*]
 *      2. SPARK_MASTER env
 *      3. spark.master system property (set by spark-submit --master)
 *      4. local[*] (IDE / bare java -jar)
 *
 * S3 (env overrides; defaults match docker-compose SeaweedFS):
 * S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_PATH
 */
object S3Example {
  // SLF4J → Log4j2. Пишет в stderr драйвера, не в stdout. Не логировать внутри map/foreach.
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val master = args.headOption
      .orElse(sys.env.get("SPARK_MASTER"))
      .getOrElse(sys.props.getOrElse("spark.master", "local[*]"))

    val endpoint = sys.env.getOrElse("S3_ENDPOINT", "http://localhost:8333")
    val accessKey = sys.env.getOrElse("AWS_ACCESS_KEY_ID", "spark")
    val secretKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "sparksecret")
    val basePath = sys.env.getOrElse("S3_PATH", "s3a://spark/learn").stripSuffix("/")
    val outPath = s"$basePath/out-parquet"

    val spark = SparkSession.builder()
      .appName("spark-learn-s3")
      .master(master)
      // Какой FileSystem открывает пути s3a://. Без этого Hadoop не знает схему s3a.
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      // Куда ходить вместо AWS. Для SeaweedFS это локальный S3 API.
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      // Регион AWS SDK. У кастомного endpoint его нет; us-east-1 — заглушка, без неё SDK падает.
      .config("spark.hadoop.fs.s3a.endpoint.region", sys.env.getOrElse("AWS_REGION", "us-east-1"))
      // URL вида http://host/bucket/key, а не http://bucket.host/key.
      // Нужно для localhost/MinIO/SeaweedFS: виртуальный хост bucket.localhost не резолвится.
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      // HTTP, не HTTPS. Локальный SeaweedFS без TLS.
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      // Не искать ключи в instance profile / default chain, а брать access/secret ниже.
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      // Access key пользователя S3 (локально: spark из s3.json).
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      // Secret key того же пользователя (локально: sparksecret).
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      // Не сверять ETag/версию объекта при повторном чтении.
      // На S3-compatible сторах метаданные часто не как у AWS — иначе ложные ошибки «файл изменился».
      .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
      // S3A directory committer: файлы пишутся сразу в бакет, видимыми их делает job commit
      // через multipart complete, без rename каталога (на S3 rename медленный и неатомарный).
      .config("spark.hadoop.fs.s3a.committer.name", "directory")
      // Spark SQL должен использовать облачный протокол commit, а не классический FileOutputCommitter.
      // Класс из spark-hadoop-cloud; без этого JAR-а — ClassNotFoundException.
      .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
      )
      // То же для Parquet: DataFrame.write.parquet(...) идёт через S3A committer, не через rename.
      .config(
        "spark.sql.parquet.output.committer.class",
        "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"
      )
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      log.info("Spark {}, master={}, appId={}", spark.version, sc.master, sc.applicationId)
      log.info("s3 endpoint={} path={}", endpoint, outPath)

      val df = spark.range(0, 20).select(
        col("id"),
        (col("id") * 10).as("value")
      )

      try {
        df.write.mode("overwrite").parquet(outPath)
        val reread = spark.read.parquet(outPath)
        val n = reread.count()
        log.info("wrote and read back {} rows", Long.box(n))
        reread.orderBy("id").show()
      } catch {
        case e: Exception =>
          log.error("S3 write/read failed: {}", outPath, e)
          throw e
      }
    } finally {
      spark.stop()
    }
  }
}
