package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.visualization.BarChart
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}

object VisualizationMain extends App with Serializable {
    val trans = new SimpleCategoryTransformer()
    trans.categories(LanguageCategory.LATIN).foreach(println(_))
  trans.categories(LanguageCategory.ARABIC).foreach(println(_))
  trans.categories(LanguageCategory.CYRILLIC).foreach(println(_))

//  var spark = SparkSession
//    .builder()
//    .appName("Spark SQL basic example")
//    .master("local[1]")
//    .getOrCreate()
//  BasicVisualizer(spark,"C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clustering Results\\settlementsConfig_3_result").draw()

}