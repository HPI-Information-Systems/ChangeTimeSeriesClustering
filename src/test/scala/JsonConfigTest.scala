import de.hpi.data_change.time_series_similarity.Clustering
import org.apache.spark.sql.SparkSession
//import org.json4s.native.JsonMethods._
import org.scalatest.FlatSpec

class JsonConfigTest extends FlatSpec {

  val spark = SparkSession.builder().appName("Unit Test").master("local[2]").getOrCreate()

  "Json Config" should "correctly set the parameters" in {
    val clusterer = new Clustering(spark)
    //3 keys:
    clusterer.setParams("src/main/resources/configs/local/N_60.json")
    assert(clusterer.aggregationTimeUnit == "Days")
    assert(clusterer.numIterations == 150)
    assert(clusterer.featureExtraction == "raw")
    assert(clusterer.seed == 1337)
    assert(clusterer.clusteringAlg == "KMeans")
    assert(clusterer.transformation == "None")
    assert(clusterer.aggregationGranularity == 14)
    assert(clusterer.numClusters == 20)
  }

  "Json Config" should "work with local Postgresql Database" in {
    val clusterer = new Clustering(spark)
    clusterer.setParams("src/main/resources/configs/local/demo_psql.json")
    clusterer.user = "dummy"
    clusterer.pw = "dummy"
    clusterer.clustering()
  }

}
