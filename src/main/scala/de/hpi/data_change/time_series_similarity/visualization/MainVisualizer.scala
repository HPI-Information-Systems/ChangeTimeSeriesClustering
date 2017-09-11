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

  private val resultPathNormal = "/home/leon/Documents/data/wikidata/results/clusteringResults/"
  private var resultPathLocal = "/home/leon/Documents/data/wikidata/results/localResults/"
  private val resultPathLogY = "/home/leon/Documents/data/wikidata/results/clusteringResultsLogY/"
  private val settlementsPath = "/home/leon/Documents/data/wikidata/settlements/dump.csv"
  private val entity_yearly_6 = "settlements_config4"
  private val entity_monthly_6 = "settlements_config9"
  private val entity_daily_6 = "settlements_config14"
  private val property_yearly_6 = "settlements_config19"
  private val property_monthly_6 = "settlements_config24"
  private val property_daily_6 = "settlements_config29"

  private val local_entity_yearly_LogY_6 = "nullsettlements_config4"
  private val local_entity_monthly_LogY_6 = "nullsettlements_config9"

  private val local_entity_yearly_6 = "nullsettlements_config4_withoutLog"
  private val local_entity_monthly_6 = "nullsettlements_config9_withoutLog"


  var resultPath = resultPathLocal
  val visualizer = new BasicVisualizer(spark, resultPath)

  //single run visualization

  detailedInfo(resultPath,local_entity_monthly_6);
  //multiple Run Visualization:
  //giveFilterOverview()
  //drawDiagramsLogY
  //drawDiagramsNormal
  //printOverviewTable()

  def detailedInfo(resultPath: String, configName: String) = {
    val singleResultPath = resultPath + File.separator + configName + File.separator
    val individualVisualizer = new IndividualVisualizer(spark,singleResultPath)
    individualVisualizer.drawClusteringCentroids()
    individualVisualizer.drawClusterCategoryBarchart()
    individualVisualizer.printClusterRepresentatives()
  }

  def drawDiagramsLogY = {
    resultPath = resultPathLogY
    //localCentroids
    drawDiagrams
  }

  private def drawDiagramsNormal = {
    resultPath = resultPathNormal
    //localCentroids
    drawDiagrams
  }

  //BasicVisualizer(spark, filePath).draw()

  private def drawDiagrams = {
    drawCentroids("settlements_config2", 0, 0)
    drawCentroids("settlements_config3", 0, 370)
    drawCentroids("settlements_config4", 0, 700)

    drawCentroids("settlements_config7", 1000, 0)
    drawCentroids("settlements_config8", 1000, 370)
    drawCentroids("settlements_config9", 1000, 700)
  }

  private def localCentroids = {
    //drawCentroids("nullsettlements_config9", 0, 0)
    drawCentroids("nullsettlements_config4", 0, 0)
  }

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