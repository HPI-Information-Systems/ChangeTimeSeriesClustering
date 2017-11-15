package de.hpi.data_change.time_series_similarity.io

import java.io.{File, FileInputStream, ObjectInputStream}

import de.hpi.data_change.time_series_similarity.configuration.ClusteringConfig
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class ResultIO {
}
object ResultIO{

  def readSettlementsCategoryMap() = {
    val in = new ObjectInputStream(new FileInputStream(DataIO.getSettlementsCategoryMapFile))
    val categoryMap = in.readObject().asInstanceOf[mutable.Map[String,mutable.Set[String]]]
    categoryMap
  }

  def readFullCategoryMap() = {
    val in = new ObjectInputStream(new FileInputStream(DataIO.getFullCategoryMapFile))
    val categoryMap = in.readObject().asInstanceOf[mutable.Map[String,mutable.Set[String]]]
    categoryMap
  }

  def loadConfig(filePath: String):ClusteringConfig = {
    val configFile = new File(filePath).listFiles().filter( f => f.getName.endsWith(".xml") && f.getName.contains("config")).head
    new ClusteringConfig(configFile.getAbsolutePath)
  }

  def loadClusteringResult(spark: SparkSession, filePath: String) = {
    spark.read.json(filePath + File.separator + "result" + File.separator)
  }

  def loadKMeansModel(filePath: String) = {
    KMeansModel.load(filePath + File.separator + "model" + File.separator)
  }

  def getConfigIdentifier(resultDir:String) = {
    new File(resultDir).listFiles().filter( f => !f.isDirectory && f.getName.endsWith(".xml")).head.getName.split("\\.")(0)
  }
}
