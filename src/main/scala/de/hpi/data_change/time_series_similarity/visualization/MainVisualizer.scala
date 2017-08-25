package de.hpi.data_change.time_series_similarity.visualization

import java.io.{File, FileWriter, PrintWriter}

import de.hpi.data_change.time_series_similarity.configuration.ClusteringConfig
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

import scala.collection.mutable.ListBuffer

class MainVisualizer(spark: SparkSession) extends Serializable {

  BasicVisualizer(spark, "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clustering Results\\settlements_config17\\result").draw()


}