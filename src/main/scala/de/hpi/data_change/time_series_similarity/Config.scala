package de.hpi.data_change.time_series_similarity

import org.codehaus.jackson.map.ObjectMapper

case class Config(jsonString:String) extends Serializable {

    @transient lazy private val config = new ObjectMapper().readTree(jsonString)
    @transient lazy private val clusteringAlgObject = config.get(CLUSTERING_ALGORITHM)

    private val AGGREGATION_GRANULARITY = "granularity"
    private val AGGREGATION_TIME_UNIT = "unit"
    private val QUERY = "query"
    private val URL = "url"
    private val CLUSTERING_ALGORITHM = "clusteringAlgorithm"
    private val CONFIG_IDENTIFIER = "configIdentifier"
    private val DRIVER = "driver"
    private val PW = "pw"
    private val USER = "user"
    private val TRANSFORMATION = "transformations"
    private val ALGORITHM_NAME = "name"
    private val K = "k"
    private val MAXITER = "maxIter"
    private val SEED = "seed"
    private val LOCAL = "local"

    //TODO: fix this check!
    //assert ( ChronoUnit.values().exists(s => s.toString == config.get(AGGREGATION_TIME_UNIT).getTextValue))

    val user = config.get(USER).getTextValue
    val password = config.get(PW).getTextValue
    val driver = config.get(DRIVER).getTextValue
    val url = config.get(URL).getTextValue
    val configIdentifier = config.get(CONFIG_IDENTIFIER).getTextValue
    val query = config.get(QUERY).getTextValue
    val aggregationGranularity = config.get(AGGREGATION_GRANULARITY).getIntValue
    val timeUnit = config.get(AGGREGATION_TIME_UNIT).getTextValue
    val clusteringAlgorithm = clusteringAlgObject.get(ALGORITHM_NAME).getTextValue
    val k = clusteringAlgObject.get(K).getIntValue
    val maxIter = clusteringAlgObject.get(MAXITER).getIntValue
    val seed = clusteringAlgObject.get(SEED).getIntValue
    val isLocal = if(config.has(LOCAL)) config.get(LOCAL).getBooleanValue else false

    val transformation = scala.collection.JavaConversions.asScalaIterator(config.get(TRANSFORMATION).getElements).toList //TODO: expect a list here
        .map( n => n.getTextValue)
}
