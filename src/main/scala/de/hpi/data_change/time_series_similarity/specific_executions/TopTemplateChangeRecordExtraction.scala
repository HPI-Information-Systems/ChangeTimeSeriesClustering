package de.hpi.data_change.time_series_similarity.specific_executions

import java.sql.Timestamp

import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

object TopTemplateChangeRecordExtraction extends App with Serializable{

  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .master("local[2]")
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  import org.apache.spark.sql.functions.lower
  val clusterer = new Clustering("","",spark)
  val stuff = clusterer.getChangeRecordDataSet(args(0))
  //re-write:
  val filtered = individualFilter(stuff)
  filtered.write.csv("/home/leon/Documents/researchProjects/wikidata/data/lol/")

  def individualFilter(dataset2: Dataset[ChangeRecord]): Dataset[ChangeRecord] = {
    //implicit def encoder: Encoder[(String,String,ChangeRecord)] = org.apache.spark.sql.Encoders.kryo[(String,String,ChangeRecord)]
    val dataset = dataset2.map(cr => ChangeRecord(cr.entity.toLowerCase,cr.property,cr.value,cr.timestamp))
    val topTemplates = Set("infobox settlement",
      "infobox album",
    "infobox person",
    "infobox football biography",
    "infobox musical artist",
    "infobox film",
    "infobox single",
    "infobox actor",
    "infobox french commune",
    "infobox company",
    "infobox football biography 2",
    "infobox book",
    "infobox television",
    "infobox military person",
    "infobox nrhp",
    "infobox school",
    "infobox vg",
    "infobox officeholder",
    "infobox uk place",
    "infobox commune de france",
    "infobox radio station",
    "infobox road",
    "infobox mlb player",
    "infobox television episode",
    "infobox indian jurisdiction",
    "infobox_nrhp",
    "infobox_company",
    "infobox writer",
    "infobox university",
    "infobox city",
    "infobox military unit",
    "infobox german location",
    "infobox mountain",
    "infobox military conflict",
    "infobox scientist",
    "infobox airport",
    "infobox ice hockey player",
    "infobox_film",
    "infobox cvg",
    "infobox nfl player",
    "infobox football club",
    "infobox station",
    "infobox software")
    var templates = clusterer.getArbitraryQueryResult(clusterer.url,"SELECT * FROM templates_infoboxes").as[(String,String)]
    templates = templates.as("template")
    val changerecords = dataset.as("changeRecord")
    val joined = templates.join(changerecords,lower(col("template.entity")) === lower(col("changeRecord.entity"))).as[(String,String,String,String,String,Timestamp)]
    val filtered = joined.filter(cr => cr._1 == "zehlendorf (berlin)")
    filtered.collect().foreach(println(_))
    val extracted =joined.map(r => r._2).distinct().collect().toSet
    topTemplates.foreach(s => println(s + " | " + extracted.contains(s)))
    joined.filter(t => topTemplates.contains(t._2)).map{case (_,_,e,p,v,t) => ChangeRecord(e,p,v,t)}.distinct() //{case (entity,template,cr) => topTemplates.contains(template) }
  }
}
