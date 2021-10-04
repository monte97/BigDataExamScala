package it.unibo.big

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{col, from_json, get_json_object}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object Es1_structuredRawGEO_2 {

  def main(args: Array[String]) = {
    println("starting v2")

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
    sparkSession.sparkContext.setLogLevel("ERROR")
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    val hiveContext = sparkSession.sqlContext

    println("Hadoop version: " + org.apache.hadoop.util.VersionInfo.getVersion)


    program(sparkSession, hiveContext)
  }

  def program(session: SparkSession, context: SQLContext): Unit ={
    val inputStream = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "137.204.74.56:7777")
      .option("auto.offset.reset", "latest")
      .option("enable.auto.commit", "true")
      .option("subscribe", "raw")
      .option("timestampFormat", "yyyy-MM-dd'T'hh:mm:ss:SSSSSSZ")
      .load()
    inputStream.printSchema()

    //https://stackoverflow.com/questions/57138507/how-to-get-only-values-from-kafka-sources-to-spark
    //https://sparkbyexamples.com/spark/spark-most-used-json-functions-with-examples/#get_json_object
    val df = inputStream.selectExpr("CAST(value AS STRING)")
      //in questi casi devo leggere una tupla (chiave, valore)
      .withColumn("id", functions.json_tuple(functions.col("value"),"id"))
      .withColumn("type", functions.json_tuple(functions.col("value"),"type"))
      .filter("type == 'Thermometer'")
      //in questo caso devo leggere un oggetto più complesso -> devo specificare un path
      .withColumn("temperature", get_json_object(col("value"),"$.Temperature.value"))
      .withColumn("latitude", get_json_object(col("value"),"$.Latitude.value").cast("Double"))
      .withColumn("longitude", get_json_object(col("value"),"$.Longitude.value").cast("Double"))
      .drop("value")
      .createTempView("thermometers")

    /**
     * Read geojson, create dataframe and view, then run a simple query
     */
    val inputLocation = "src/main/resources/testOut3.geojson"
    val allowTopologyInvalidGeometries = false // Optional
    val skipSyntaxInvalidGeometries = true // Optional
    val municipalityRDD = GeoJsonReader.readToGeometryRDD(session.sparkContext, inputLocation, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
    val municipalityDF = Adapter.toDf(municipalityRDD, session)
    municipalityDF.printSchema()
    municipalityDF.createOrReplaceTempView("municipality")

    val selectMunicipalityGeom = context.sql(
      """
        |select name, ST_GeomFromText(geometry) as geometry
        |from municipality
        |""".stripMargin)
      .createOrReplaceTempView("municipality")

    val selectRawDevices = context.sql(
      """
        |select ST_Point(CAST(longitude as Decimal(24, 20)), CAST(latitude as Decimal(24,20))) as point, id
        |from thermometers
        |""".stripMargin)
      .createTempView("geo_thermometers")

    /*
    val print = context.sql(
      """
        |select *
        |from geo_thermometers
        |""".stripMargin)
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .start()
      .awaitTermination()
     */

    /**
     * https://postgis.net/docs/ST_GeomFromText.html
     * http://sedona.incubator.apache.org/archive/api/sql/GeoSparkSQL-Predicate/#st_contains
     */
    val joinMunicipality = context.sql(
      """
        |select T.id, M.name
        |from geo_thermometers as T, municipality as M
        |where ST_Contains(M.geometry, T.point)
        |""".stripMargin)
      .createTempView("join_municipality")

    val print = context.sql(
      """
        |select *
        |from join_municipality
        |""".stripMargin)
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      //.start()
      //.awaitTermination()

    context.sql(
      """
        |select id, point
        |from geo_thermometers
        |where ST_Contains(point, point)
        |""".stripMargin)
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .start()
      .awaitTermination()




    println("before test")
    val test  = context.sql(
      """
        |select name
        |from municipality
        |where ST_Contains(geometry, geometry)
        |""".stripMargin)
      .show(2)
    println("after first test")

    /**
     * 44.32186131302602, 12.234047881131605
     * https://sedona.apache.org/api/sql/Constructor/#st_pointfromtext
     */
    val test2 = context.sql(
      """
        |Select name, geometry
        |from municipality
        |where ST_Contains(geometry, ST_PointFromText('44.3218,12.2340',','))
        |""".stripMargin)
      //.show(1)
  }
}