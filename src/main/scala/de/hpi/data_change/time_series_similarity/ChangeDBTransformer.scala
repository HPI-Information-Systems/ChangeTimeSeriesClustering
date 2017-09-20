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

  def fixString(entity: String) = "\"" +  entity.replace("\"","\\\"") + "\""

  def addQuotes(str: String) = "\"" + str + "\""

  def transformWikiDataNewlineValues() = {
    val repairedFile = DataIO.getFullWikidataSparkCompatibleFile()
    val out = DataIO.getBZ2CompressedOutputStream(repairedFile)
    val readFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.ALL)
    //val printer = new CSVPrinter(new OutputStreamWriter(out),writeFormat)
    val printer = new PrintWriter(new OutputStreamWriter(out))
    val decompressedFileStream = DataIO.getOriginalFullWikidataFileStream
    val records = readFormat.parse(decompressedFileStream).iterator()
    var numRow = 1
    val del = ","
    while(records.hasNext){
      val r = records.next()
      assert(r.size() == 4)
      val rec = new ChangeRecord(r)
      if(numRow % 1000000 == 0){
        println("processed "+ numRow + " change records")
      }
      numRow += 1
//      println(numRow)
//      println(rec)
      printer.println(fixString(rec.entity) +del+ fixString(rec.property).replace("\r\n","<newline>").replace("\n","<newline>").take(100000) +del+ "\"newVal\"" +del+ addQuotes(rec.timestampAsString))
    }
    printer.flush()
    printer.close()
    //val parser = org.apache.commons.csv.CSVParser. (CSVFormat.DEFAULT)
  }
}
