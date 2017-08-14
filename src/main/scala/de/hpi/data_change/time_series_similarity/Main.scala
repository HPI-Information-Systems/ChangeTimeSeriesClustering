package de.hpi.data_change.time_series_similarity

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql._

import scala.collection.Map

object Main extends App with Serializable{

  //constants for execution:
  val minNumNonZeroYValues = 3
  val granularity = TimeGranularity.Monthly
  val groupingKey = GroupingKey.Entity
  if(args.length<1){
    throw new AssertionError("No file path specified - terminating")
  }
  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  if(args.length==2 && args(1) == "-local" ){
    sparkBuilder = sparkBuilder.master("local[4]")
  }
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]

  new SimilarityExecutor(minNumNonZeroYValues,granularity,groupingKey,spark,args(0)).calculatePairwiseSimilarity()

}
