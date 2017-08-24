package de.hpi.data_change.time_series_similarity.visualization

import java.io.{File, FileWriter, PrintWriter}

import de.hpi.data_change.time_series_similarity.ClusteringConfig
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

import scala.collection.mutable.ListBuffer

class MainVisualizer(spark: SparkSession) extends Serializable {
  //    val trans = new SimpleCategoryTransformer()
  //    trans.categories(LanguageCategory.LATIN).foreach(println(_))
  //  trans.categories(LanguageCategory.ARABIC).foreach(println(_))
  //  trans.categories(LanguageCategory.CYRILLIC).foreach(println(_))


  import spark.implicits._
  //BasicVisualizer(spark, "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clustering Results\\settlements_config17\\result").draw()
  var results = scala.collection.mutable.ListBuffer[Seq[String]]()
  val header = List("Config","Group By","Agg by","k","Cluster 1", "Cluster 2","Cluster 3","Cluster 4","Cluster 5","Cluster 6")
  for (filePath <- new File("C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clustering Results\\").listFiles()) {
    val configNum = filePath.getAbsolutePath.split("config")(1)
    val config = new ClusteringConfig(filePath + "\\settlements_config" +configNum+".xml")
    val clusteringResult = spark.read.csv(filePath.getAbsolutePath + "\\result")
    val grouped = clusteringResult.groupByKey(r => "cluster_" + r.getAs[Int](1))
    val clusterSizes = grouped.mapGroups { case (cluster, it) => (cluster, it.size) }.collect()
    val res = scala.collection.mutable.ListBuffer[String]()
    res.append(config.configIdentifier)
    res.append(config.groupingKey.toString)
    res.append(config.granularity.toString)
    //res.append(config.minNumNonZeroYValues.toString)
    res.append(config.clusteringAlgorithmParameters("k"))
    res.appendAll(clusterSizes.map{ case (cluster, size) => size }.sorted.reverse.map(i => i.toString))
    while(res.size!=10) res.append("")
    results.append(res)
  }
  results = results.sortBy( vals => (vals(1),vals(2),vals(3)))
  results.prepend(header)
  println(format(results))
  saveToFile(results,"src/main/resources/results/clusteringResult.csv")

  def saveToFile(results: Seq[Seq[String]], filePath: String) = {
    val pr = new PrintWriter(new FileWriter(filePath))
    val lines = results.map(vals => vals.reduce( (s1,s2) => s1+","+s2))
    lines.foreach(pr.println(_))
    pr.close()
  }

  def format(table: Seq[Seq[Any]]) = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]) = colSizes map { "-" * _ } mkString("+", "+", "+")


}