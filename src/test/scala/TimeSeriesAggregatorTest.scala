import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.configuration.{FeatureExtractionMethod, GroupingKey, TimeGranularity, TimeSeriesFilter}
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest._

class TimeSeriesAggregatorTest extends FlatSpec{

  var spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[2]")
    .getOrCreate()

  "Normalizted Time Series" should "correctly map to normalized Value" in {

    val filePath = "src/main/resources/testdata/changeDB.csv"
    val timeSeries = TimeSeriesAggregator(spark,null,TimeGranularity.MonthOfYear,GroupingKey.Entity)
      .aggregateToTimeSeries(filePath,FeatureExtractionMethod.EntireTimeSeriesNormalized)
      .collect()
    assert( timeSeries.size == 1)
    val ts = timeSeries(0)
    assert (ts.timeSeries.forall( y => y <=1))
    assert(ts.timeSeries(0) == 1.0 && ts.timeSeries(1) == 1.0)
    assert(ts.timeSeries(2) == 0.5)
    assert(ts.timeSeries(3) == 0.25 && ts.timeSeries(4) == 0.25)
  }
}
