package de.hpi.data_change.time_series_similarity.data_mining

import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}


case class TimeSeriesClusterer(spark: SparkSession, filePath: String,minNumNonZeroYValues:Int,granularity:TimeGranularity.Value,groupingKey:GroupingKey.Value) {

  def buildClusters(numClusters:Int,numIterations:Int,resultDir:String) = {
    val timeSeries = new TimeSeriesAggregator(spark,minNumNonZeroYValues,granularity,groupingKey).aggregateToTimeSeries(filePath)
    val rdd = timeSeries.rdd.map(ts => RowFactory.create(ts.name,Vectors.dense(ts.getClusteringFeatures())))
    val fields = Array(DataTypes.createStructField("name",DataTypes.StringType,false),DataTypes.createStructField("features",VectorType,false))
    val schema = new StructType(fields)
    val finalDf = spark.createDataFrame(rdd,schema)
    val clusteringAlg = new KMeans()
      .setFeaturesCol("features")
      .setK(numClusters)
      .setMaxIter(numIterations)
      .setPredictionCol("assignedCluster")
    val kmeansModel = clusteringAlg.fit(finalDf)
    val resultDF = kmeansModel.transform(finalDf)
    println("Cost is: " + kmeansModel.computeCost(finalDf))
    println("Starting to save results")
    resultDF.select("name","assignedCluster").write.csv(resultDir + "KMeans_k=" + numClusters + "_maxIter=" + numIterations + "_result")
    kmeansModel.write.save(resultDir + "KMeans_k=" + numClusters + "_maxIter=" + numIterations + "_model.spark")
  }


  private def tryWithInProgramData = {
    //TODO: why does this work, but it does not work above??
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(training)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
