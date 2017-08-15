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
    val result = executor.calculatePairwiseSimilarity()
    val assembler = new VectorAssembler()
      .setInputCols(Array(result.columns(2)))
      .setOutputCol("features")
    val newResult3 = assembler.transform(result)
    val numClusters = 2
    val numIterations = 20
    val clusteringAlg = new KMeans()
      .setFeaturesCol("features")
      .setK(numClusters)
      .setMaxIter(numIterations)
      .setPredictionCol("assignedCluster")
    val clusteringResult = clusteringAlg.fit(newResult3)
    val WSSSE = clusteringResult.computeCost(newResult3)
    println("Within Set Sum of Squared Errors = " + WSSSE)


    //implicit def multiDimEncoder:Encoder[MultiDimensionalTimeSeries] = Encoders.kryo[MultiDimensionalTimeSeries]

//    //TODO:
//    case class ClusteringInput(ts1: MultiDimensionalTimeSeries,ts2: MultiDimensionalTimeSeries,features: Vector)
//    //var newResult = result.map{case (e1,e2,dist) => (e1,e2,Vectors.dense(dist))}
//    //val newResult = result.map{case (e1,e2,dist) => ClusteringInput(e1,e2,Vectors.dense(dist))}
//    val first = result.head()
//    assert(first._1.name == "e1")
//    assert(first._2.name == "e2")
//    //assert(first._3 == 2.0)
//    val newResult2 = result.toDF().map(r => RowFactory.create(r.getAs[MultiDimensionalTimeSeries](0),r.getAs[MultiDimensionalTimeSeries](1),Vectors.dense(r.getDouble(2))))


  }
}
