import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.data.TimeSeries
import de.hpi.data_change.time_series_similarity.dba.DBA
import net.sf.javaml.core.DenseInstance
import net.sf.javaml.distance.dtw.DTWSimilarity
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

import scala.collection.mutable
import scala.util.Random

class DTWDistanceTest extends FlatSpec{

  val sim = new DTWSimilarity()

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

  "Modified DBA" should "output the same values as original DBA" in {

  }


}
