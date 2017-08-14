import de.hpi.data_change.time_series_similarity.{SimilarityExecutor, TimeGranularity}
import org.scalatest._
import de.hpi.data_change.time_series_similarity.GroupingKey
import de.hpi.data_change.time_series_similarity.Main.args
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{Encoders, SparkSession}

class SimilarityExecutorTest extends FlatSpec{
  "calculatePairwiseSimilarity" should "order results by distance" in {

    var sparkBuilder = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[1]")
    val spark = sparkBuilder.getOrCreate()
    val executor = new SimilarityExecutor(0,TimeGranularity.Monthly,GroupingKey.Entity,spark,"src/main/resources/testData.csv")
    var result = executor.calculatePairwiseSimilarity()
    implicit def vectorEncoder = Encoders.kryo[DenseVector]
    //TODO:
    //result = result.map(t => (t._1,t._2,Vectors.dense(t._3)))
    //result = result.map{case (e1,e2,dist) => (e1,e2,Vectors.dense(dist))}
    val first = result.head()
    assert(first._1.name == "e1")
    assert(first._2.name == "e2")
    //assert(first._3 == 2.0)
    val numClusters = 2
    val numIterations = 20
    val clusteringAlg = new KMeans()
      .setFeaturesCol(result.columns(2))
      .setK(numClusters)
      .setMaxIter(numIterations)
      .setPredictionCol("assignedCluster")
    val clusteringResult = clusteringAlg.fit(result)
    val WSSSE = clusteringResult.computeCost(result)
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }
}
