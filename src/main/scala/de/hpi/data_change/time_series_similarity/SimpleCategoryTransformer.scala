package de.hpi.data_change.time_series_similarity

import java.io.{BufferedReader, FileReader, InputStreamReader}

class SimpleCategoryTransformer {

  def extractCategories() = {
    val br = new BufferedReader(new FileReader("src/main/resources/unicode/UnicodeData.txt"))
    var line = br.readLine()
    while(line!=null){
      if(line.split(";")(1).startsWith("LATIN")){
        //TODO - this is a lation symbol
      }


      line = br.readLine()
    }
  }

  extractCategories()

}
