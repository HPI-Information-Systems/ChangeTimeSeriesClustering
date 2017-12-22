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
  filtered.write.csv("/home/leon/Documents/researchProjects/wikidata/data/top_templates_new/")

  def individualFilter(dataset2: Dataset[ChangeRecord]): Dataset[ChangeRecord] = {
    //implicit def encoder: Encoder[(String,String,ChangeRecord)] = org.apache.spark.sql.Encoders.kryo[(String,String,ChangeRecord)]
    val dataset = dataset2.map(cr => ChangeRecord(cr.entity.toLowerCase,cr.property,cr.value,cr.timestamp))
    val actualString = " infobox settlement\n infobox album\n infobox person\n infobox football biography\n infobox musical artist\n infobox film\n infobox single\n infobox company\n infobox french commune\n infobox nrhp\n infobox book\n infobox television\n infobox military person\n infobox video game\n infobox school\n infobox officeholder\n infobox uk place\n infobox baseball biography\n infobox radio station\n infobox road\n infobox television episode\n infobox indian jurisdiction\n infobox writer\n infobox university\n infobox military unit\n infobox german location\n infobox mountain\n infobox military conflict\n infobox scientist\n infobox airport\n infobox ice hockey player\n infobox cvg\n infobox nfl biography\n infobox football club"
    val topTemplates = actualString.split("\n").map(s => s.trim).toSet
    var templates = clusterer.getArbitraryQueryResult(clusterer.url,"SELECT * FROM templates_infoboxes").as[(String,String)]
    templates = templates.as("template")
    val changerecords = dataset.as("changeRecord")
    val joined = templates.join(changerecords,lower(col("template.entity")) === lower(col("changeRecord.entity"))).as[(String,String,String,String,String,Timestamp)]
    val extracted =joined.map(r => r._2).distinct().collect().toSet
    topTemplates.foreach(s => println(s + " | " + extracted.contains(s)))
    joined.filter(t => topTemplates.contains(t._2)).map{case (_,_,e,p,v,t) => ChangeRecord(e,p,v,t)}.distinct() //{case (entity,template,cr) => topTemplates.contains(template) }
  }
}
