package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.visualization.MainVisualizer1
import org.apache.spark.sql.SparkSession

object VisualizationMain1 extends App{

  var spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[1]")
    .getOrCreate()
  new MainVisualizer1(spark)
}
