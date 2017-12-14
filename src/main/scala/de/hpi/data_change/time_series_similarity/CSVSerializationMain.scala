package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.visualization.CSVSerializer
import org.apache.spark.sql.SparkSession

object CSVSerializationMain extends App{

  var spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[1]")
    .getOrCreate()
  val sparkResultPath = "/home/leon/Documents/researchProjects/wikidata/results/infobox/"
  val csvResultPath = "/home/leon/Documents/researchProjects/wikidata/results/csvResults/"
  new CSVSerializer(spark, sparkResultPath,csvResultPath).addGroundTruth().serializeToCsv()
}
