package de.hpi.data_change.time_series_similarity

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.SparkSession

object JsonMain extends App{
  val isLocal = args.length==3 && args(2) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    if(isLocal) {
      sparkBuilder = sparkBuilder.master("local[2]")
    }
  val spark = sparkBuilder.getOrCreate()

  val id = args(0)
  val jsonPath = args(1)
  val exploration = new Clustering(spark)
  exploration.setParams(jsonPath)
  exploration.writeResults = false
  //TODO: change exploration.setDBQueryAsDataSource(config.get("query").getTextValue,false)
  exploration.setGrouper(cr => Seq(cr.entity,cr.property))
  exploration.clustering()
}
