#!/usr/bin/env bash
# Download extra jars for $SPARK_HOME/jars (S3 / ClickHouse / GraphFrames).
# Spark core/sql/mllib already ship in the distribution — not copied here.
#
#   ./installation/fetch-spark-jars.sh
#   cp installation/spark-*-jars/*.jar "$SPARK_HOME/jars/"
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"
  export JAVA_HOME
fi

prop() { mvn -q -DforceStdout help:evaluate -Dexpression="$1"; }
SPARK_VERSION="$(prop spark.version)"
SCALA_BIN="$(prop scala.binary.version)"
CH_BIN="$(prop clickhouse.spark.binary.version)"
CH_CONN="$(prop clickhouse.spark.connector.version)"
CH_JDBC="$(prop clickhouse.jdbc.version)"
GF_VER="$(prop graphframes.version)"

# Hadoop version aligned with spark-hadoop-cloud_2.13:${SPARK_VERSION}
HADOOP_AWS_VERSION="3.5.0"
AWS_SDK_BUNDLE_VERSION="2.35.4"
S3_ACCEL_VERSION="1.3.1"
WILDFLY_OPENSSL_VERSION="2.2.5.Final"

CLOUD_DIR="$ROOT/installation/spark-cloud-jars"
CH_DIR="$ROOT/installation/spark-clickhouse-jars"
GF_DIR="$ROOT/installation/spark-graphframes-jars"

rm -rf "$CLOUD_DIR" "$CH_DIR" "$GF_DIR"
mkdir -p "$CLOUD_DIR" "$CH_DIR" "$GF_DIR"

WORK="$ROOT/installation/.fetch-jars-work"
rm -rf "$WORK"
mkdir -p "$WORK"
DEP_PLUGIN="org.apache.maven.plugins:maven-dependency-plugin:3.7.0"

copy_one() {
  mvn -q "$DEP_PLUGIN:copy" -Dartifact="$1" -DoutputDirectory="$2"
}

echo "Versions: spark=$SPARK_VERSION scala=$SCALA_BIN ch=$CH_CONN gf=$GF_VER"

# --- ClickHouse (runtime is shaded; JDBC "all" is fat) ---
echo "==> ClickHouse"
copy_one "com.clickhouse.spark:clickhouse-spark-runtime-${CH_BIN}_${SCALA_BIN}:${CH_CONN}" "$CH_DIR"
copy_one "com.clickhouse:clickhouse-jdbc:${CH_JDBC}:jar:all" "$CH_DIR"

# --- GraphFrames (+ graphx + datasketches) ---
echo "==> GraphFrames"
cat >"$WORK/graphframes.pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.sparklearn</groupId>
  <artifactId>fetch-graphframes-jars</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>io.graphframes</groupId>
      <artifactId>graphframes-spark4_${SCALA_BIN}</artifactId>
      <version>${GF_VER}</version>
    </dependency>
  </dependencies>
</project>
EOF
mvn -q -f "$WORK/graphframes.pom.xml" "$DEP_PLUGIN:copy-dependencies" \
  -DoutputDirectory="$GF_DIR" \
  -DincludeScope=runtime \
  -DexcludeGroupIds=org.apache.spark,org.scala-lang,org.slf4j

# --- Cloud / S3 only (SeaweedFS / MinIO / AWS). Skip Azure/GCS/Aliyun/Huawei. ---
echo "==> spark-hadoop-cloud / S3"
copy_one "org.apache.spark:spark-hadoop-cloud_${SCALA_BIN}:${SPARK_VERSION}" "$CLOUD_DIR"

cat >"$WORK/s3.pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.sparklearn</groupId>
  <artifactId>fetch-s3-jars</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-aws</artifactId>
      <version>${HADOOP_AWS_VERSION}</version>
      <exclusions>
        <exclusion><groupId>org.wildfly.openssl</groupId><artifactId>*</artifactId></exclusion>
        <!-- Already in Spark / Hadoop client on the cluster -->
        <exclusion><groupId>org.apache.hadoop</groupId><artifactId>hadoop-common</artifactId></exclusion>
        <exclusion><groupId>org.apache.hadoop</groupId><artifactId>hadoop-annotations</artifactId></exclusion>
        <exclusion><groupId>org.apache.hadoop</groupId><artifactId>hadoop-auth</artifactId></exclusion>
        <exclusion><groupId>org.apache.hadoop</groupId><artifactId>hadoop-client-api</artifactId></exclusion>
        <exclusion><groupId>org.apache.hadoop</groupId><artifactId>hadoop-client-runtime</artifactId></exclusion>
        <exclusion><groupId>com.fasterxml.jackson.core</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>com.fasterxml.jackson.dataformat</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>org.apache.httpcomponents</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>joda-time</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>commons-logging</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>software.amazon.awssdk</groupId>
      <artifactId>bundle</artifactId>
      <version>${AWS_SDK_BUNDLE_VERSION}</version>
      <exclusions>
        <exclusion><groupId>*</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>software.amazon.s3.analyticsaccelerator</groupId>
      <artifactId>analyticsaccelerator-s3</artifactId>
      <version>${S3_ACCEL_VERSION}</version>
      <exclusions>
        <!-- Optional telemetry; keep the accelerator jar itself -->
        <exclusion><groupId>io.opentelemetry</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>io.opentracing</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>org.hdrhistogram</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>org.reactivestreams</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>com.fasterxml.jackson.core</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.wildfly.openssl</groupId>
      <artifactId>wildfly-openssl</artifactId>
      <version>${WILDFLY_OPENSSL_VERSION}</version>
    </dependency>
  </dependencies>
</project>
EOF
mvn -q -f "$WORK/s3.pom.xml" "$DEP_PLUGIN:copy-dependencies" \
  -DoutputDirectory="$CLOUD_DIR" \
  -DincludeScope=runtime

rm -rf "$WORK"

echo
echo "ClickHouse -> $CH_DIR"
ls -lh "$CH_DIR"
echo
echo "GraphFrames -> $GF_DIR"
ls -lh "$GF_DIR"
echo
echo "Cloud/S3 -> $CLOUD_DIR"
ls -lh "$CLOUD_DIR"
echo
TOTAL="$(find "$CLOUD_DIR" "$CH_DIR" "$GF_DIR" -name '*.jar' | wc -l | tr -d ' ')"
SIZE="$(du -sh "$CLOUD_DIR" "$CH_DIR" "$GF_DIR" | awk '{print $1,$2}' | paste -sd ', ' -)"
echo "Total jars: $TOTAL ($SIZE)"
echo
echo "Install on every Spark node:"
echo "  cp installation/spark-*-jars/*.jar \"\$SPARK_HOME/jars/\""
