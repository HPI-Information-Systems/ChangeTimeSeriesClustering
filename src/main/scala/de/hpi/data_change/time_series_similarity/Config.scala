package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.dba.ClusteringAlgSerializer
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import scala.io.Source

case class Config(
                     user: String,
                     pw: String,
                     driver: String,
                     url: String,
                     configIdentifier: String,
                     query: String,
                     granularity: Int,
                     unit: String,
                     transformation: Seq[String],
                     local:Boolean,
                     clusteringAlgorithm:ClusteringAlg
                 ) extends Serializable

case class ClusteringAlg(name: String,k: Int,maxIter: Int,seed:Long) extends Serializable;

object Config {
    implicit val formats = DefaultFormats + new ClusteringAlgSerializer

    def read(str: String) = {
        val configAsString = Source.fromFile(str).getLines().mkString("\n")
        val json = parse(configAsString)
        json.extract[Config]
    }
}