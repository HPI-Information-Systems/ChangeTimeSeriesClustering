package de.hpi.data_change.time_series_similarity.preprocessing

import java.io._
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
  val firstBatchSize = 44
  val lineBuffer = new mutable.ListBuffer[String]()

  def printFile(lineBuffer: Seq[String], resultDir: String, batchNumber: Int) = {
    println("Creating Batch " + batchNumber)
    val writer = new PrintWriter(new FileWriter(new File(resultDir + "batch_"+batchNumber+".sql")))
    lineBuffer.foreach(writer.println(_))
    writer.close()
  }

  while(line!=null){
    lineBuffer.append(line)
    if(lineNumber == firstBatchSize || lineNumber > firstBatchSize && lineBuffer.size==batchLineSize){
      printFile(lineBuffer,resultDir,batchNumber)
      batchNumber +=1
      lineBuffer.clear()
    }
    lineNumber+=1
    line = reader.readLine()
  }
  if(!lineBuffer.isEmpty){
    printFile(lineBuffer,resultDir,batchNumber)
  }
  reader.close()
}
