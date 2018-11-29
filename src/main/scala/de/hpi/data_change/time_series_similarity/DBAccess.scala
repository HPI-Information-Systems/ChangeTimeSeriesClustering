package de.hpi.data_change.time_series_similarity

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codehaus.jackson.JsonNode
import org.json4s.JsonAST.{JArray, JDouble, JObject}
import org.json4s.jackson.JsonMethods._

case class DBAccess(spark:SparkSession,config:Config) extends Serializable {

    import spark.implicits._

    def writeDFToDB(df: DataFrame,tableName:String) = {
        val props = new Properties()
        props.setProperty("user",config.user)
        props.setProperty("password",config.pw)
        props.setProperty("driver",config.driver)
        df.write.jdbc(config.url,tableName,props)
    }

    def writeToDB(resultDF: DataFrame, centers:Seq[Array[Double]], inputSchema:Seq[String]) = {
        //cluster centers
        var centerDF = spark.createDataset(centers
            .map(arr => {
                val centersAsJson = arr.map(d => JDouble(d)).toList
                val json = JObject("centers" -> JArray(centersAsJson))
                compact(render(json))
            }).zipWithIndex)
        centerDF.withColumnRenamed(centerDF.columns(0),"center")
            .withColumnRenamed(centerDF.columns(1),"cluster")
        //drop the time series values:
        val df1 = resultDF.toDF()
            .withColumn("rowNr",monotonically_increasing_id())
            .select("name","assignedCluster","rowNr")
        //explode key array:
        val keyArrayExploded = df1.select(
            $"rowNr",
            $"assignedCluster",
            posexplode($"name")
        )
        //use grouping and pivoting to create one column per element in the original array:
        var finalDF = keyArrayExploded.groupBy("rowNr","assignedCluster").pivot("pos").agg(first("col"))
            .drop("rowNr") //no longer needed
        //reorder the dataframe so that assignedCluster is the last column:
        val oldOrder = finalDF.columns
        val newOrder = oldOrder.slice(1, oldOrder.size) ++ Seq(oldOrder(0))
        finalDF = finalDF.select(newOrder.head, newOrder.tail: _*)
        //rename the first n-1 columns to the original input columns:
        val currentNames = finalDF.columns.slice(0, finalDF.columns.size - 1)
        val originalNames = inputSchema.slice(0, inputSchema.size - 1)
        currentNames.zip(originalNames).foreach { case (old, new_) => {
            finalDF = finalDF.withColumnRenamed(old, new_)
          }
        }
        finalDF.show()

        //do the database writing:
        writeDFToDB(finalDF,config.configIdentifier)
        writeDFToDB(centerDF.toDF(),config.configIdentifier + "_centers")
    }

    def getInputDataframe = getArbitraryQueryResult(config.url,config.query)

    private def getArbitraryQueryResult(url:String,query:String) = {
        spark.sqlContext.read.format("jdbc").
            option("url", url).
            option("driver", config.driver).
            option("useUnicode", "true").
            //option("continueBatchOnError","true").
            option("useSSL", "false").
            option("user", config.user).
            option("password", config.pw).
            option("dbtable","(" + query + ") as queryresult").
            load()
    }
}
