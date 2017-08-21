package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.ClusteringMain.{args, resultDirectory}
import de.hpi.data_change.time_series_similarity.visualization.BarChart
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}

object VisualizationMain extends App with Serializable {
  //val sampleData = List(("a","cluster 1",24),("a","cluster 2",5),("b","cluster 1",20),("b","cluster 2",70))
  //val chart = new BarChart("sample Chart relative",sampleData,true)
  //chart.draw()
  //val chartAbsolute = new BarChart("sample Chart",sampleData,false)
  //chartAbsolute.draw()
  var spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[1]")
    .getOrCreate()
  BasicVisualizer(spark,"C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clustering Results\\settlementsConfig_3_result").draw()

}