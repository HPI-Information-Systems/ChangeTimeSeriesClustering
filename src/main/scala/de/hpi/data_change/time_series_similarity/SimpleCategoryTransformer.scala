package de.hpi.data_change.time_series_similarity

import java.io.{BufferedReader, FileReader, InputStreamReader}
import java.util.stream.Collectors

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SimpleCategoryTransformer extends Serializable{

  def containsRange(list: ListBuffer[(Long, Long)], codePoint: Int): Boolean = {
    list.foreach{case (start,stop) => if(start <= codePoint && codePoint <= stop) return true}
    false
  }

  def getCategory(codePoint: Int):LanguageCategory.Value = {
    categories.foreach{case (cat,list) => if(containsRange(list,codePoint)) return cat}
    LanguageCategory.other
  }

  def getLanguage(entity: String):LanguageCategory.Value = {
    var frequencies = mutable.Map[LanguageCategory.Value,Int]()
    LanguageCategory.values.foreach(frequencies(_) = 0)
    var offset = 0
    if(entity == null){
      println("woot?")
    }
    while(offset < entity.length){
      val codePoint = entity.codePointAt(offset)
      frequencies(getCategory(codePoint)) = frequencies(getCategory(codePoint)) + 1
      offset += Character.charCount(codePoint)
    }
    frequencies.maxBy(_._2)._1
  }


  var categories = extractCategories()

  def fillmap(map: mutable.Map[LanguageCategory.Value, ListBuffer[(Long, Long)]]) = {
    LanguageCategory.values.foreach(cat => map(cat) = new ListBuffer[(Long, Long)])
  }

  def getCodepoint(line: String):Long = {
    java.lang.Long.parseLong(line.split(";")(0),16)
  }

  def extractCategories() = {
    val br = new BufferedReader(new FileReader("src/main/resources/unicode/UnicodeData.txt"))
    var line = br.readLine()
    var map:mutable.Map[LanguageCategory.Value,ListBuffer[(Long,Long)]] = mutable.Map[LanguageCategory.Value,ListBuffer[(Long,Long)]]()
    fillmap(map)
    println(map)
    var prevCategory:LanguageCategory.Value = LanguageCategory.other
    var codePointStart:Long = -1
    while(line!=null){
      val curCategory = LanguageCategory.getCategory(line)
      if(prevCategory == LanguageCategory.other) {
        if (curCategory == LanguageCategory.other) {
          //we do nothing in this case
        } else {
          //start a new interval:
          codePointStart = getCodepoint(line)
          prevCategory = curCategory
        }
      } else {
        if(curCategory == prevCategory){
          //we do nothing
        } else{
          val codePointEnd = getCodepoint(line)-1
          map(prevCategory).append((codePointStart,codePointEnd))
          //println(map)
          prevCategory = curCategory
          if(curCategory == LanguageCategory.other){
            // we do nothing
          } else{
            codePointStart = codePointEnd+1
          }
        }
      }
      line = br.readLine()
    }
    br.close()
    map
  }

}
