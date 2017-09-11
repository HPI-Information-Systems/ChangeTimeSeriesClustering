package de.hpi.data_change.time_series_similarity

import java.io._
import java.time.ZoneOffset

import breeze.io.CSVReader
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import de.hpi.data_change.time_series_similarity.io.{DataIO, ResultIO}
import org.apache.commons.csv.{CSVFormat, CSVPrinter, QuoteMode}
import org.apache.spark.sql.SparkSession

object ChangeDBTransformer extends App{

  var spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[1]")
    .getOrCreate()

  //filterWeirdChanges()

  transformWikiDataNewlineValues()

  def filterWeirdChanges() = {
    val settlementsNew = new File("/home/leon/Documents/data/wikidata/settlements/dumpNew.csv")
    val settlements = DataIO.getSettlementsFile
    val reader = new BufferedReader(new FileReader(settlements))
    val changeRecords = new TimeSeriesAggregator(spark,null,null,null).getChangeRecordDataSet(settlements.getAbsolutePath).collect()
    var prev:ChangeRecord = null

    val pr = new PrintWriter(new FileWriter(settlementsNew))
    val format = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.ALL)
    val printer = new CSVPrinter(pr, format)
    var skipped = 0
    for (cur <- changeRecords.sortBy(f => (f.entity, f.property, f.timestamp.toEpochSecond(ZoneOffset.UTC)))) {
      if(prev!=null && prev.entity == cur.entity && prev.property == cur.property && cur.value == prev.value){
        //skip line
        println("Found a line to skip")
        skipped +=1
      } else{
        printer.printRecord(cur.entity,cur.property,cur.value,cur.timestampAsString)
      }
      prev = cur
    }
    printer.close()
    println("Deleted Lines: " + skipped)
  }

  def transformWikiDataNewlineValues() = {
    val repairedFile = DataIO.getWikidataRepairedFileWithoutValue()
    val out = DataIO.getBZ2CompressedOutputStream(repairedFile)
    val format = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.ALL)
    val printer = new CSVPrinter(new OutputStreamWriter(out),format)
    val decompressedFileStream = DataIO.getOriginalFullWikidataFileStream
    val records = format.parse(decompressedFileStream).iterator()
    var numRow = 1
    while(records.hasNext){
      val r = records.next()
      val rec = new ChangeRecord(r)
      val recNew = new ChangeRecord(rec.entity,rec.property,"newVal",rec.timestamp)
      if(numRow % 1000000 == 0){
        println("processed "+ numRow + " change records")
      }
      numRow += 1
//      println(numRow)
//      println(rec)
      printer.printRecord(recNew.entity,recNew.property,recNew.value,recNew.timestampAsString)
    }
    printer.flush()
    printer.close()
    //val parser = org.apache.commons.csv.CSVParser. (CSVFormat.DEFAULT)
  }
}
