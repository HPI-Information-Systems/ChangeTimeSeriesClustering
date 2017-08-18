import org.scalatest._
import de.hpi.data_change.time_series_similarity.ClusteringMain.args
import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data_mining.PairwiseSimilarityExtractor
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.ml.linalg._

class PairwiseSimilarityExtractorTest extends FlatSpec{

  "calculatePairwiseSimilarity" should "order results by distance" in {

    var sparkBuilder = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[1]")
    val spark = sparkBuilder.getOrCreate()
    val executor = new PairwiseSimilarityExtractor(0,TimeGranularity.Monthly,GroupingKey.Entity,spark,"src/main/resources/testData.csv")
    val result = executor.calculatePairwiseSimilarity(null)
    val first = result.head()
    assert(first._1 == "e1")
    assert(first._2 == "e2")
    assert(first._3 == 2.0)
  }
}
