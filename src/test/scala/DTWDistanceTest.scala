import de.hpi.data_change.time_series_similarity.data.TimeSeries
import de.hpi.data_change.time_series_similarity.dba.DBAKMeans
import net.sf.javaml.core.DenseInstance
import net.sf.javaml.distance.dtw.DTWSimilarity
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Encoder, SparkSession}
import org.scalatest.FlatSpec

import scala.collection.mutable
import scala.util.Random

class DTWDistanceTest extends FlatSpec{

  val sim = new DTWSimilarity()
  val spark = SparkSession.builder().appName("Unit Test").master("local[2]").getOrCreate()

  "DTW" should "return the same result a reference implementation" in {
    //TODO: paths incorrect!
    //generate test sequences
    val rand = new Random(13)
    val sequences = mutable.ListBuffer.fill(10)(mutable.ListBuffer.fill(100)(rand.nextDouble()))
    for (i <- 0 until sequences.size-1) {
      for ( j <- i+1 until sequences.size) {
        sequences(i)(10) = 1000.00
        sequences(j)(50) = 1000.00
        val ts1 = TimeSeries(null,sequences(i),-1,"",null)
        val ts2 = TimeSeries(null,sequences(j),-1,"",null)
        val expected = sim.measure(new DenseInstance(sequences(i).toArray),new DenseInstance(sequences(j).toArray))
        val actual = ts1.dtwDistance(ts2)
        assert(expected == actual._1)
        //assert symmetry:
        val symmetrical = ts2.dtwDistance(ts1)
        assert(actual._1 == symmetrical._1)
        println(i + " to " + j + ": " +actual)
        println(j + " to " + i + ": " +symmetrical)
      }
    }
  }

  "Modified DBA" should "correctly classify cosinus vs tangens" in {
    implicit def changeRecordListEncoder: Encoder[(Seq[String],org.apache.spark.ml.linalg.Vector)] = org.apache.spark.sql.Encoders.kryo[(Seq[String],org.apache.spark.ml.linalg.Vector)]

    val numPoints = 100
    val random = new Random(13)
    val numSeqs = 100
    val cosinusSequences = mutable.ListBuffer[Seq[Double]]()
    val tanSequences = mutable.ListBuffer[Seq[Double]]()
    for(i <- 0 until numSeqs){
      val offset = random.nextDouble()*Math.PI
      val cosinus = (0 until numPoints).map( i => Math.cos((Math.PI*(i+offset) )/numPoints)).toList
      cosinusSequences += cosinus
      val tan = (0 until numPoints).map( i => Math.tan((Math.PI*(i+offset) )/numPoints)).toList
      tanSequences += tan
    }
    val dba = new DBAKMeans(2,50,13,spark)
    val df = spark.createDataset(cosinusSequences.map( i=> (Seq("cos"),Vectors.dense(i.toArray))) ++ tanSequences.map(i => (Seq("tan"),Vectors.dense(i.toArray))))
    var finalDF = df.toDF()
      finalDF = finalDF.withColumnRenamed(finalDF.columns(0),"name")
      .withColumnRenamed(finalDF.columns(1),"features")
    val (centers,resultDF) = dba.fit(finalDF)
    centers.foreach( println(_))
    resultDF.collect().foreach( r => println(r.getAs[Seq[String]]("name")(0) + "  " + r.getAs[Int]("assignedCluster")))

  }


}
