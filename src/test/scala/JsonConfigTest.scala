import java.io.File

import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import de.hpi.data_change.time_series_similarity.Clustering
import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.JsonAST.JObject
//import org.json4s.native.JsonMethods._
import org.scalatest.FlatSpec

class JsonConfigTest extends FlatSpec {

  val spark = SparkSession.builder().appName("Unit Test").master("local[2]").getOrCreate()

  "Json Config" should "correctly set the parameters" in {
    val clusterer = new Clustering("","",spark)
    //3 keys:
    val mapper = new ObjectMapper()
    val stuff = mapper.readTree(new File("src/main/resources/configs/testConfig.json"))
    clusterer.setParams(stuff)
    assert(clusterer.aggregationTimeUnit == "Days")
    assert(clusterer.numIterations == 150)
    assert(clusterer.featureExtraction == "raw")
    assert(clusterer.seed == 1337)
    assert(clusterer.clusteringAlg == "KMeans")
    assert(clusterer.transformation == "None")
    assert(clusterer.aggregationGranularity == 14)
    assert(clusterer.numClusters == 20)
  }

}
