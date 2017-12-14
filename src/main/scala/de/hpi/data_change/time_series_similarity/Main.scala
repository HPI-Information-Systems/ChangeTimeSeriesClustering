package de.hpi.data_change.time_series_similarity

import org.apache.spark.sql.SparkSession

object Main extends App{

  val isLocal = args.length==4 && args(3) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
  if(isLocal) {
    sparkBuilder = sparkBuilder.master("local[2]")
  }
  val spark = sparkBuilder.getOrCreate()
  val clusterer = new Clustering(args(1),args(2),spark)
  clusterer.setFileAsDataSource(args(0))
  clusterer.clustering()

}
