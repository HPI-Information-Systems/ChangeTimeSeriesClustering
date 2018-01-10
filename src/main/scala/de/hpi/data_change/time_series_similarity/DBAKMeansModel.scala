package de.hpi.data_change.time_series_similarity

import org.apache.spark.sql.DataFrame

class DBAKMeansModel(centers: Seq[Array[Double]]) {

  def serializeCenters(filePath: String) = {

  }

  def transform(finalDF: DataFrame): _root_.org.apache.spark.sql.Dataset[_root_.org.apache.spark.sql.Row] = {
    null
  }

}
