package de.hpi.data_change.time_series_similarity.io

import java.io.File

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.SparkSession

class ResultIO {
}
object ResultIO{
  def loadClusteringResult(spark: SparkSession, filePath: String) = {
    spark.read.load(filePath + File.separator + "result" + File.separator)
  }

  def loadKMeansModel(filePath: String) = {
    KMeansModel.load(filePath + File.separator + "model" + File.separator)
  }

  def getConfigIdentifier(resultDir:String) = {
    new File(resultDir).listFiles().filter( f => !f.isDirectory && f.getName.endsWith(".xml")).head.getName.split("\\.")(0)
  }
}
