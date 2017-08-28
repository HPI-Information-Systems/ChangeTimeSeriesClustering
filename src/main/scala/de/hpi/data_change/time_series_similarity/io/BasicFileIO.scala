package de.hpi.data_change.time_series_similarity.io

import java.io.{BufferedReader, FileReader}

import scala.collection.mutable.ListBuffer

class BasicFileIO {

  def readCSV(filePath:String):ListBuffer[List[String]]  ={
    val results = ListBuffer[List[String]]()
    val br = new BufferedReader(new FileReader(filePath))
    var line = br.readLine()
    while(line!=null){
      results.append(line.split(",").toList)
      line = br.readLine()
    }
    results
  }
}
