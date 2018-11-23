package de.hpi.data_change.time_series_similarity

import org.apache.spark.sql.SparkSession

/**
  * Main entry to the clustering framework
  * First parameter is a .json file containing most user-configurable options
  * Other User-Defined Options (Grouping and filter on the groups) are defined here
  */
object Main extends App with Serializable{

  val isLocal = args.length==2 && args(1) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
  if(isLocal) {
    sparkBuilder = sparkBuilder.master("local[2]")
  }
  val spark = sparkBuilder.getOrCreate()
//  val clusterer = new Clustering(spark)
//  clusterer.setParams(args(0))
//  clusterer.grouper = (cr => Seq(cr.entity))
//  clusterer.groupFilter = (ds => ds.filter(t => t._2.size>50))
  clusterer.clustering()
}
