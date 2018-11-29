package de.hpi.data_change.time_series_similarity

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.codehaus.jackson.map.ObjectMapper

/**
  * Main class, however not for the clustering framework as a standalone, but integrated into the DBChex-tool.
  * Unfamiliar users should use Main instead
  */
object JsonMain extends App{
  val jsonPath = args(0)
  val configAsJsonString = new ObjectMapper().readTree(new File(jsonPath)).toString

  val config = new Config(configAsJsonString)
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    if(config.isLocal) {
      sparkBuilder = sparkBuilder.master("local[2]")
    }
  val spark = sparkBuilder.getOrCreate()

  val exploration = new Clustering(spark,configAsJsonString)
  exploration.clustering()
}
