package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.visualization.MainVisualizer
import org.apache.spark.sql.SparkSession

object VisualizationMain extends App{

  var spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[2]")
    .getOrCreate()
  new MainVisualizer(spark)
}
