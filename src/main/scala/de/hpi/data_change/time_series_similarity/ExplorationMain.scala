package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.ClusteringMain.args
import de.hpi.data_change.time_series_similarity.data_mining.DataStatisticsExploration
import org.apache.spark.sql.SparkSession

object ExplorationMain extends App with Serializable {

  if(args.length<1){
    throw new AssertionError("No file path specified - terminating")
  }
  if(args.length<2){
    throw new AssertionError("No result directory specified - terminating")
  }
  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  if(args.length==3 && args(2) == "-local" ){
    sparkBuilder = sparkBuilder.master("local[4]")
  }
  val spark = sparkBuilder.getOrCreate()
  new DataStatisticsExploration(spark,args(0)).explore(args(1))
}
