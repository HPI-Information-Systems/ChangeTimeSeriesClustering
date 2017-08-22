package de.hpi.data_change.time_series_similarity;

object LanguageCategory extends Enumeration{

  def getCategory(line: String) = {
    val catName = line.split(";")(1).split("\\s")(0).toUpperCase
    if(values.exists(_.toString == catName)) {
      withName(catName)
    } else{
      other
    }
  }

  val LATIN,CYRILLIC,ARABIC,other = Value
}
