package de.hpi.data_change.time_series_similarity

import java.io.File

import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.map.ObjectMapper

object JsonMain extends App{
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .master("local[2]")
  val spark = sparkBuilder.getOrCreate()
  val targetDir = "/home/leon/Desktop/test/"
  val id = args(0)
  val jsonPath = args(1)
  val exploration = new Clustering(targetDir,id,spark)
  val config = new ObjectMapper().readTree(new File(jsonPath))
  exploration.setParams(config)
  exploration.setDBQueryAsDataSource(config.get("query").getTextValue,false)
  exploration.setGrouper(cr => Seq(cr.entity,cr.property))
  exploration.clustering()
}
