import de.hpi.data_change.time_series_similarity.LocalExplorationMain
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class GroupFromDatabaseReadTest extends FlatSpec {

  val spark = SparkSession.builder().appName("Unit Test").master("local[2]").getOrCreate()

  "Query to database" should "return the correct results" in {
    val clusterer = new LocalExplorationMain("","",spark)
    //3 keys:
    var querystring = "select * from test_3_keys"
    //database config
    var databaseURL = "jdbc:postgresql://localhost/test_changeclustering"
    var res = clusterer.getArbitraryQueryResult(databaseURL,querystring)
    assert(res.schema.size == 4)
    assert(res.count() == 8)
    //2 keys:
    querystring = "select * from test_2_keys"
    databaseURL = "jdbc:postgresql://localhost/test_changeclustering"
    res = clusterer.getArbitraryQueryResult(databaseURL,querystring)
    assert(res.schema.size == 3)
    assert(res.count() == 9)
    //transformArbitraryDatasetToGroup
  }

  def toRegex(s: String): String = s.replace("|","\\|")

  "Grouping by arbitrary number of keys" should "interpret the first n-1 attributes as keys, the last one as timestamp" in {
    val clusterer = new LocalExplorationMain("","",spark)
    //3 keys:
    var querystring = "select * from test_3_keys"
    //database config
    var databaseURL = "jdbc:postgresql://localhost/test_changeclustering"
    var res = clusterer.getArbitraryQueryResult(databaseURL,querystring)
    val groups = clusterer.transformArbitraryDatasetToGroup(res).collect().toMap
    assert(groups.keySet.size == 5)
    assert(groups.keySet.forall( s => s.split(toRegex(clusterer.KeySeparator)).size == 3))
    assert(groups(List("a1","b1","c1").mkString(clusterer.KeySeparator)).size == 3)
    assert(groups(List("a2","b2","c2").mkString(clusterer.KeySeparator)).size == 2)
    assert(groups(List("a2","b2","c1").mkString(clusterer.KeySeparator)).size == 1)
    assert(groups(List("a2","b1","c2").mkString(clusterer.KeySeparator)).size == 1)
    assert(groups(List("a1","b2","c2").mkString(clusterer.KeySeparator)).size == 1)
  }


}
