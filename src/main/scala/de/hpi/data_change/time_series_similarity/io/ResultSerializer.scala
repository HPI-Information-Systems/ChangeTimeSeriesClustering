package de.hpi.data_change.time_series_similarity.io

import de.hpi.data_change.time_series_similarity.configuration.ClusteringConfig
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ResultSerializer(spark:SparkSession, resultDf: Dataset[Row], model: KMeansModel) extends Serializable{

  import spark.implicits._

  val hadoopInteraction = new HadoopInteraction()

  def serialize(resultDirectory:String,configIdentifier:String,config:ClusteringConfig):Seq[String] = {
    resultDf.write.save(resultDirectory + configIdentifier + "/result")
    model.save(resultDirectory + configIdentifier + "/model")
    //aggregated results:
    val tablePath = resultDirectory + "results.CSV.txt"
    if(!hadoopInteraction.exists(tablePath)){
      val header = List("Config", "Group By", "Agg by", "k", "WSSSE", "Cluster 1", "Cluster 2", "Cluster 3", "Cluster 4", "Cluster 5", "Cluster 6")
      hadoopInteraction.appendLineToCsv(header,tablePath)
    }
    val res = scala.collection.mutable.ListBuffer[String]()
    res.append(config.configIdentifier)
    res.append(config.groupingKey.toString)
    res.append(config.granularity.toString)
    //res.append(config.minNumNonZeroYValues.toString)
    res.append(config.clusteringAlgorithmParameters("k"))
    res.append(model.computeCost(resultDf).toString)
    val grouped = resultDf.groupByKey(r => "cluster_" + r.getAs[Int]("assignedCluster"))
    val clusterSizes = grouped.mapGroups { case (cluster, it) => (cluster, it.size) }.collect()
    res.appendAll(clusterSizes.map { case (cluster, size) => size }.sorted.reverse.map(i => i.toString))
    while (res.size != 11) res.append("")
    hadoopInteraction.appendLineToCsv(res,tablePath)
    res
  }
}
