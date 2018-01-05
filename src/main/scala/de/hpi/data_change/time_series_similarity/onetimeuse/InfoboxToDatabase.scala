package de.hpi.data_change.time_series_similarity.onetimeuse

import java.util.Properties

import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.specific_executions.TopTemplateChangeRecordExtraction.args
import org.apache.spark.sql.SparkSession

object InfoboxToDatabase extends App{

  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .master("local[2]")
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  import org.apache.spark.sql.functions.lower
  val clusterer = new Clustering("","",spark)
  val stuff = clusterer.getChangeRecordDataSet("/home/leon/Documents/researchProjects/wikidata/data/top_templates_new/")
  val props = new Properties()
  props.setProperty("user","dummy")
  props.setProperty("password","dummy")
  props.setProperty("driver","org.postgresql.Driver")
  stuff.write.jdbc("jdbc:postgresql://localhost/changedb","wikipedia_infobox_changes",props)
}
