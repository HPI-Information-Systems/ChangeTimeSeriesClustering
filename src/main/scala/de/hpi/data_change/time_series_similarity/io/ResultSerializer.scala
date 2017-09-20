package de.hpi.data_change.time_series_similarity.io

import de.hpi.data_change.time_series_similarity.configuration.ClusteringConfig
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ResultSerializer(spark:SparkSession, resultDf: Dataset[Row], model: KMeansModel) extends Serializable{

  import spark.implicits._


  def serialize(resultDirectory:String,configIdentifier:String,config:ClusteringConfig,hadoopInteraction: HadoopInteraction) = {
    resultDf.write.save(resultDirectory + configIdentifier + "/result")
    model.save(resultDirectory + configIdentifier + "/model")
  }
}
