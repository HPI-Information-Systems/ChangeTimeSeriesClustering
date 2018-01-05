package de.hpi.data_change.time_series_similarity.specific_executions

import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.specific_executions.WikipediaEntityClusteringMain_top_34_normalize_kMediod_dtw.args
import org.apache.spark.sql.SparkSession

object EntitySampler extends App{

  val source = "/home/leon/Documents/researchProjects/wikidata/data/top_templates_new/"
  val target = "/home/leon/Documents/researchProjects/wikidata/data/top_templates_new_sample/"

  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .config("spark.kryoserializer.buffer.max","700MB")
    sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  val clusterer = new Clustering("","",spark)
  val a = clusterer.getChangeRecordDataSet(source)
  val list = a.map(cr => cr.entity).distinct().collect()

  val indices = scala.util.Random.shuffle((1 until list.size).toList).take(list.size*1/100)
  val toTake = indices.map(list(_)).toSet
  a.filter(cr => toTake.contains(cr.entity)).write.csv(target)
}
