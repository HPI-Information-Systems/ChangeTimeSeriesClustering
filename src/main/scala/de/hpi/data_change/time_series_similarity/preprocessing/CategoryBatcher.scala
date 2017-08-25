package de.hpi.data_change.time_series_similarity.preprocessing

import java.io._
import java.time.LocalDateTime
import java.util.zip.GZIPInputStream

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CategoryBatcher extends App{
  //C:\Users\Leon.Bornemann\Documents\Database Changes\Data\wikidata\wiki categories
  val filePath = "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Data\\wikidata\\wiki categories\\enwiki-20170801-categorylinks.sql.gz"
  val resultDir = "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Data\\wikidata\\wiki categories\\enwiki-20170801-categorylinks-batches\\"
  val reader = new BufferedReader(new InputStreamReader(new GZIPInputStream((new FileInputStream(filePath)))))
  var line = reader.readLine()
  var batchNumber = 0
  var lineNumber:Long = 1
  var batchLineSize = 1000
  val firstBatchSize = 40
  val lineBuffer = new mutable.ListBuffer[String]()

  def printFile(lineBuffer: Seq[String], resultDir: String, batchNumber: Int) = {
    println("Creating Batch " + batchNumber)
    val writer = new PrintWriter(new FileWriter(new File(resultDir + "batch_"+batchNumber+".sql")))
    lineBuffer.foreach(writer.println(_))
    writer.close()
  }

  //first batch
  while(lineNumber<=firstBatchSize){
    lineBuffer.append(line)
    lineNumber+=1
    line = reader.readLine()
  }
  printFile(lineBuffer,resultDir,batchNumber)

  val writer = new PrintWriter(new FileWriter(new File(resultDir + "batch_"+1+".sql")))
  println("starting new batch")
  while(line!=null){
    if(lineNumber %100 == 0){
      println(LocalDateTime.now() + ":  transferred 100 lines")
    }
    writer.println(line)
    lineNumber+=1
    line = reader.readLine()
  }

  reader.close()
}
