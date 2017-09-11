package de.hpi.data_change.time_series_similarity.data

import de.hpi.data_change.time_series_similarity.CategoryExtractor.spark
import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import org.apache.spark.sql.SparkSession

class BasicSparkQuerier(spark:SparkSession) {

  import spark.implicits._

  def getDistinctEntities(filepath:String): Seq[String] ={
    val res = TimeSeriesAggregator(spark,null,TimeGranularity.Yearly,GroupingKey.Entity).getChangeRecordDataSet(filepath)
    val entities = res.map(cr => cr.entity).distinct().collect()
    entities
  }
}
