package de.hpi.data_change.time_series_similarity

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codehaus.jackson.JsonNode

case class DBAccess(spark:SparkSession,config:Config) extends Serializable {

    import spark.implicits._

    def writeToDB(resultDF: DataFrame,centers:Seq[Array[Double]],inputSchema:Seq[String]) = {
        val centerDF = spark.createDataset(centers.zipWithIndex)

        val df1 = resultDF.toDF()
            .withColumn("rowNr",monotonically_increasing_id())
            .select("name","assignedCluster","rowNr")
        df1.show()
        val splitAndExploded = df1.select(
            $"rowNr",
            $"assignedCluster",
            posexplode($"name")
        )
        splitAndExploded.show()
        val dfNew = splitAndExploded.groupBy("rowNr","assignedCluster").pivot("pos").agg(first("col"))
        dfNew.show()
        val finalDF = dfNew.drop("rowNr")

        val oldOrder = finalDF.columns
        val newOrder = oldOrder.slice(1, oldOrder.size) ++ Seq(oldOrder(0))
        val finalDFReordered = finalDF.select(newOrder.head, newOrder.tail: _*)

        val oldCols = finalDFReordered.columns.slice(0, finalDFReordered.columns.size - 1)
        val originalNames = inputSchema.slice(0, inputSchema.size - 1)
        var finalColumnsRenamed = finalDFReordered

        oldCols.zip(originalNames).foreach { case (old, new_) => {
            finalColumnsRenamed = finalColumnsRenamed.withColumnRenamed(old, new_)
        }
        }
        finalColumnsRenamed.show()


        val toWrite = resultDF.map(r => {
            val keyArray = r.getAs[Seq[String]](0)
            val cluster = r.getAs[Int]("assignedCluster")
            (keyArray.mkString(Clustering.KeySeparator),cluster)
        }).toDF()
            .withColumn("rowNr",monotonically_increasing_id())

//        val finalFinalDF = getResultDF(toWrite,inputSchema)


        println("dummy output simulating database write")
        val props = new Properties()
        props.setProperty("user",config.user)
        props.setProperty("password",config.password)
        props.setProperty("driver",config.driver)
        finalColumnsRenamed.write.jdbc(config.url,config.configIdentifier,props)
        centerDF.write.jdbc(config.url,config.configIdentifier + "_centers",props)
        toWrite.write.jdbc(config.url,config.configIdentifier + "_old",props)
    }

    private def getResultDF(toWrite:DataFrame,inputSchema:Seq[String]) = {
        //split by key separator and
        val toWriteColumns = toWrite.columns
        val clusterCol = $"${toWriteColumns(1)}"
        val columnToSplit = $"${toWriteColumns(0)}"
        val splitAndExploded = toWrite.select(
            clusterCol,
            $"rowNr",
            split(columnToSplit, Clustering.KeySeparator).alias("parts"),
            posexplode(split(columnToSplit, Clustering.KeySeparator))
        )
        val concatenated = splitAndExploded
            .drop($"col").select(
            $"rowNr",
            concat(lit("letter"), col("pos").cast("string")).alias("name"),
            expr("parts[pos]").alias("val"),
            clusterCol.alias("Cluster")
        )
        val clusterColName = concatenated.columns(concatenated.columns.length - 1)
        //groupBy should be group by row number!
        val pivoted = concatenated.groupBy("rowNr", clusterColName).pivot("name").agg(first("val"))
        val oldOrder = pivoted.columns
        val newOrder = Seq(oldOrder(0)) ++ oldOrder.slice(2, oldOrder.size) ++ Seq(oldOrder(1))
        val columnsReordered = pivoted.select(newOrder.head, newOrder.tail: _*)
            .drop("rowNr")
        columnsReordered.show()
        val oldCols = columnsReordered.columns.slice(0, columnsReordered.columns.size - 1)
        val originalNames = inputSchema.slice(0, inputSchema.size - 1)
        assert(oldCols.length == originalNames.size)
        var finalColumnsRenamed = columnsReordered
        oldCols.zip(originalNames).foreach { case (old, new_) => {
            finalColumnsRenamed = finalColumnsRenamed.withColumnRenamed(old, new_)
            }
        }
        finalColumnsRenamed = finalColumnsRenamed.withColumnRenamed(pivoted.columns(pivoted.columns.length - 1), "Cluster")
        finalColumnsRenamed
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
            option("password", config.password).
            option("dbtable","(" + query + ") as queryresult").
            load()
    }
}
