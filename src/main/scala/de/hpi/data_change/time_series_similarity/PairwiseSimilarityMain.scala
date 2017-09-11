package de.hpi.data_change.time_series_similarity

import java.io.File

import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data_mining.{PairwiseSimilarityExtractor, TimeSeriesClusterer}
import org.apache.spark.sql._

object PairwiseSimilarityMain extends App with Serializable{

  //constants for execution:
  val minNumNonZeroYValues = 0
  val granularity = TimeGranularity.Monthly
  val groupingKey = GroupingKey.Entity
  //clustering specific:
  val k = 2
  val maxIter = 100
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
  val resultDir = if(args(1).endsWith(File.separator)) args(1) else args(1) + File.separator
  new PairwiseSimilarityExtractor(null,granularity,groupingKey,spark,args(0)).calculatePairwiseSimilarity(args(1))
}
