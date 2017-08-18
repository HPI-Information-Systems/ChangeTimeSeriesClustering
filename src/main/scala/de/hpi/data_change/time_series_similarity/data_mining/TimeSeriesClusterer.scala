package de.hpi.data_change.time_series_similarity.data_mining

import de.hpi.data_change.time_series_similarity.configuration.{ClusteringAlgorithm, FeatureExtractionMethod, GroupingKey, TimeGranularity}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}


case class TimeSeriesClusterer(spark: SparkSession, filePath: String,minNumNonZeroYValues:Int,granularity:TimeGranularity.Value,groupingKey:GroupingKey.Value,configIdentifier:String) {

  def kmeansClustering(params:Map[String,String], resultDir: String, finalDf:Dataset[Row]) = {
    val numClusters = params.get("k").get.toInt
    val numIterations = params.get("maxIter").get.toInt
    val clusteringAlg = new KMeans()
      .setFeaturesCol("features")
      .setK(numClusters)
      .setMaxIter(numIterations)
      .setPredictionCol("assignedCluster")
    val kmeansModel = clusteringAlg.fit(finalDf)
    val resultDF = kmeansModel.transform(finalDf)
    println("Cost is: " + kmeansModel.computeCost(finalDf))
    println("Starting to save results")
    if(resultDir!=null) {
      resultDF.select("name", "assignedCluster").write.csv(resultDir + configIdentifier + "_result")
      kmeansModel.write.save(resultDir + configIdentifier + "_model.spark")
    }
  }


  def buildClusters(params:Map[String,String], resultDir:String) = {
    val algorithm = ClusteringAlgorithm.withName(params.get("name").get)
    val featureExtractionMethod = FeatureExtractionMethod.withName(params.get("FeatureExtractionMethod").get)
    assert(featureExtractionMethod == FeatureExtractionMethod.EntireTimeSeries)
    val timeSeries = new TimeSeriesAggregator(spark,minNumNonZeroYValues,granularity,groupingKey).aggregateToTimeSeries(filePath)
    val rdd = timeSeries.rdd.map(ts => RowFactory.create(ts.name,Vectors.dense(ts.getClusteringFeatures())))
    val fields = Array(DataTypes.createStructField("name",DataTypes.StringType,false),DataTypes.createStructField("features",VectorType,false))
    val schema = new StructType(fields)
    val finalDf = spark.createDataFrame(rdd,schema)
    algorithm match {
      case ClusteringAlgorithm.KMeans => kmeansClustering(params,resultDir,finalDf)
      case _ => throw new AssertionError("unknown clustering algorithm specified")
    }
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
