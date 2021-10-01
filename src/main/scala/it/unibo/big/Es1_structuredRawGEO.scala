package it.unibo.big

import org.apache.spark
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object Es1_structuredRawGEO {

  def main(args: Array[String]) = {
    println("starting")
    /*
    *   IN WINDOWS
    * - download winutils.exe (https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip)
    * - set HADOPP_HOME
    *   - in intellij
    *     - edit configuration -> enviorment variables = "HADOOP_HOME=C:\hadoop"
    * - set 777 permission on /tmp/hive
    *     - C:\hadoop\bin\winutils.exe chmod 777 \tmp\hive
    *
    * */

    val sparkSession = SparkSession.builder()
      .master("local[2]") // Delete this if run in cluster mode
      .appName("unit test") // Change this to a proper name
      .config("spark.broadcast.compress", "false")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .config("spark.io.compression.codec", "lzf")
      .config("spark.serializer", classOf[KryoSerializer].getName) // Enable GeoSpark custom Kryo serializer
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .enableHiveSupport()
      .getOrCreate()
    println("session created")
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    val hiveContext = sparkSession.sqlContext

    println("Hadoop version: " + org.apache.hadoop.util.VersionInfo.getVersion)


    program(sparkSession, hiveContext)
  }

  def program(session: SparkSession, context: SQLContext): Unit ={
    val inputStream = session
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", "137.204.74.56:7777")
      .option("auto.offset.reset", "latest")
      .option("enable.auto.commit", "true")
      .option("subscribe", "raw")
      .load()
    inputStream.printSchema()
    val fiwareSchema = StructType(
      List(
        StructField("id", StringType, nullable = false),
        StructField("type", StringType, nullable = false),
        StructField("Latitude", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", DoubleType, nullable = false)
        )), nullable = false),
        StructField("Longitude", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", DoubleType, nullable = false)
        )), nullable = false),
        StructField("Temperature", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", DoubleType, nullable = false)
        )), nullable = false),
        StructField("Location", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", StringType, nullable = false)
        )), nullable = false),
        StructField("Status", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", BooleanType, nullable = false)
        )), nullable = false),
        StructField("Time", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", DoubleType, nullable = false)
        )), nullable = false),
        StructField("TimeInstant", StructType(List(
          StructField("type", StringType, nullable = false),
          StructField("value", StringType, nullable = false)
        )))
      )
    )

    val df = inputStream
      .selectExpr("CAST(value AS STRING)")
      .toDF("value")
      .select(from_json(col("value"), fiwareSchema).alias("tmp")).select("tmp.*")
      .withColumn("timestampExtracted", col("TimeInstant.value").cast("timestamp"))
      .where("type == 'Thermometer'")
      .withColumn("latitude", col("Latitude.value").cast("double"))
      .withColumn("longitude", col("Longitude.value").cast("double"))
      .withColumn("temperature", col("Temperature.value").cast("double"))
      .withWatermark("timestampExtracted", "5 seconds")

    df.createOrReplaceTempView("thermometers")
    df.printSchema()


    /**
     * Read geojson, create dataframe and view, then run a simple query
     */
    val inputLocation = "src/main/resources/testOut3.geojson"
    val allowTopologyInvalidGeometries = true // Optional
    val skipSyntaxInvalidGeometries = false // Optional
    val municipalityRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
    val municipalityDF = Adapter.toDf(municipalityRDD, session)
    municipalityDF.printSchema()
    municipalityDF.createOrReplaceTempView("municipality")
    context.sql(
      """
        |Select *
        |from municipality
        |""".stripMargin)
      .show(10)

    val selectRawDevices = context.sql(
      """
        |select *
        |from thermometers
        |""".stripMargin)
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .start()
      .awaitTermination()



  }
}