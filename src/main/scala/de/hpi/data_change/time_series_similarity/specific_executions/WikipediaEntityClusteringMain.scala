package de.hpi.data_change.time_series_similarity.specific_executions

import java.sql.Timestamp
import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.Main.args
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{max, min}

import scala.util.Random

object WikipediaEntityClusteringMain extends App with Serializable{
  val isLocal = args.length==4 && args(3) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .config("spark.kryoserializer.buffer.max","700MB")
  if(isLocal) {
    sparkBuilder = sparkBuilder.master("local[2]")
  }
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  val clusterer = new Clustering(args(1),args(2),spark)
  clusterer.setFileAsDataSource(args(0))
  //fixed params
  clusterer.setGrouper(cr => Seq(cr.entity))
  clusterer.transformation = "normalize"
  clusterer.numClusters = 43
  clusterer.setTimeBorders(java.sql.Timestamp.valueOf("2001-01-18 00:00:01.0"),java.sql.Timestamp.valueOf("2017-08-02 00:00:01.0"))
  //variable params:
  clusterer.aggregationTimeUnit = "DAYS"
  clusterer.aggregationGranularity = 60
  clusterer.seed = 13
  clusterer.minGroupSize = 50
  clusterer.clustering()
}
