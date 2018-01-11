package de.hpi.data_change.time_series_similarity.specific_executions

import java.sql.Timestamp
import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.Main.args
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.specific_executions.TestData.clusterer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{max, min}

import scala.util.Random

object TestData extends App with Serializable{
  val isLocal = args.length==3 && args(2) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .config("spark.kryoserializer.buffer.max","700MB")
  if(isLocal) {
    sparkBuilder = sparkBuilder.master("local[2]")
  }
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  //KMeans
  var clusterer = new Clustering(args(1),"testKMeans",spark)
  setParams(clusterer)
  clusterer.clustering()
  //DBAKMEANS
  clusterer = new Clustering(args(1),"testKMeansDBA",spark)
  setParams(clusterer)
  clusterer.clusteringAlg = "DBAKMeans"
  clusterer.clustering()

  def setParams(clusterer: Clustering) = {
    clusterer.setFileAsDataSource(args(0))
    //fixed params
    clusterer.addGroundTruth = false
    clusterer.setGrouper(cr => Seq(cr.entity))
    clusterer.transformation = List()
    clusterer.numClusters = 2
    clusterer.setTimeBorders(java.sql.Timestamp.valueOf("2014-02-21 00:00:00"),java.sql.Timestamp.valueOf("2014-02-21 12:00:00"))
    //variable params:
    clusterer.aggregationTimeUnit = "Minutes"
    clusterer.aggregationGranularity = 10
    clusterer.seed = 13
    clusterer.minGroupSize = 0
  }
}
