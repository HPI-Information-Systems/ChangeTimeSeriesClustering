package de.hpi.data_change.time_series_similarity.onetimeuse

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import de.hpi.data_change.time_series_similarity.onetimeuse.InfoboxHeatmapExample.args
import org.apache.spark.sql.SparkSession

import scala.util.Random

object TestDataGenerator extends App{

  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()

  import spark.implicits._
  val random = new Random()
  val entities = List("Traffic Light 1","Traffic Light 2","Traffic Light 3","Traffic Light 4")
  val values = List("Red","Yellow","Green")
  var start:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 00:00:00") //2014-02-21_Movies_changes
  var end:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 12:00:00") //2017-07-14

  def getRandomTuple(entities: List[String], values: List[String], start: Timestamp, end: Timestamp) = {
    val entity = entities(random.nextInt(entities.size))
    val property = "color"
    val value = values(random.nextInt(values.size))
    val timestamp = start.toLocalDateTime.plusHours(random.nextInt(12)).plusMinutes(random.nextInt(60)).plusSeconds(random.nextInt(60))
    (entity,property,value,Timestamp.valueOf(timestamp))
  }

  val crs = List.fill(1000)(getRandomTuple(entities,values,start,end))
  spark.createDataset(crs).coalesce(1).write.csv("src/main/resources/testdata/trafficLights.csv")
}
