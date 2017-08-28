package de.hpi.data_change.time_series_similarity.visualization

import java.io.{File, FileWriter, PrintWriter}

import de.hpi.data_change.time_series_similarity.configuration.ClusteringConfig
import de.hpi.data_change.time_series_similarity.io.{BasicFileIO, ResultIO}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

import scala.collection.mutable.ListBuffer

class MainVisualizer(spark: SparkSession) extends Serializable {

  private val filePath = "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clustering Results"
  printOverviewTable()
  //BasicVisualizer(spark, filePath).draw()

  def printOverviewTable(): Unit ={
    val pathToCSV = filePath + File.separator + "results.CSV.txt";
    val csvContent = new BasicFileIO().readCSV(pathToCSV)
    val table = List(csvContent.head) ++  csvContent.drop(1).sortBy(c => (c(1),c(2),c(3)) )
    println(new TabluarResultFormatter().format(table))
    val singleResult = filePath + File.separator + "settlements_config0" + File.separator
    new BasicVisualizer(spark,filePath).drawClusteringCentroids(singleResult,ResultIO.getConfigIdentifier(singleResult))
  }
}