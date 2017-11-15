package de.hpi.data_change.time_series_similarity.visualization

import java.io.{File, FileWriter, PrintWriter}

import com.sun.xml.internal.ws.assembler.MetroConfigName
import de.hpi.data_change.time_series_similarity.configuration.{ClusteringConfig, GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import de.hpi.data_change.time_series_similarity.io.{BasicFileIO, ResultIO}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

import scala.collection.mutable.ListBuffer

class MainVisualizer(spark: SparkSession) extends Serializable {
  import spark.implicits._
  val resultPathLocal = "/home/leon/Documents/researchProjects/imdb/localResults/"
  val configName = "imdb_first_test"


  var resultPath = resultPathLocal
  val visualizer = new BasicVisualizer(spark, resultPath)

  //single run visualization
  detailedInfo(resultPath,configName);
  //detailedInfo(resultPath,local_property_monthOfYear);

//  detailedInfo(resultPath,local_entity_monthly_6);
//  detailedInfo(resultPath,local_entity_monthly_LogY_6);
  //multiple Run Visualization:
  //giveFilterOverview()
  //drawDiagramsLogY
  //drawDiagramsNormal
  //printOverviewTable()

  def detailedInfo(resultPath: String, configName: String) = {
    val singleResultPath = resultPath + File.separator + configName + File.separator
    val individualVisualizer = new IndividualVisualizer(spark,singleResultPath)
    individualVisualizer.drawClusteringCentroids()
    //individualVisualizer.drawClusterCategoryBarchart()
    individualVisualizer.printClusterRepresentatives()
  }


  //BasicVisualizer(spark, filePath).draw()


  private def drawCentroids(configName:String, x:Int, y:Int) = {
    val result = resultPath + File.separator + configName + File.separator
    visualizer.drawClusteringCentroids(result, ResultIO.getConfigIdentifier(result),x,y)
    visualizer.drawClusterCategoryBarchart(result)
  }

  def getConfigId(str: String): Int = {
    if(str.length == "settlements_config0".size){
      return str.substring(str.length-1,str.length).toInt
    } else{
      str.substring(str.length-2,str.length).toInt
    }
  }

  def printOverviewTable(): Unit ={
    val pathToCSV = resultPath + File.separator + "results.CSV.txt";
    val csvContent = new BasicFileIO().readCSV(pathToCSV)
    val table = List(csvContent.head) ++  csvContent.drop(1).sortBy(c => getConfigId(c(0)) )
    println(new TabluarResultFormatter().format(table))
   }
}