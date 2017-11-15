package de.hpi.data_change.time_series_similarity.io

import java.io._

import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.CompressorStreamFactory


class DataIO {

}

object DataIO{
  def getIMDBDir() = "/home/leon/Documents/researchProjects/imdb/cleaned/"


  def getTempFileStream = {
    val file = new File("/home/leon/Desktop/malformatted")
    val fin = new FileInputStream(file)
    new BufferedReader(new InputStreamReader(fin))
  }

  def getConfigTargetDir(): String = "/home/leon/Documents/researchProjects/wikidata/configs/"

  def getHDFSWikidataFilePath(): String = "/users/leon.bornemann/data/20120323-en-updates_new_full_repaired_without_Value.csv"


  def processedResultDir() = "/home/leon/Documents/researchProjects/wikidata/results/preProcessed/"

  def getOriginalFullWikidataFileStream = {
    val file = new File("/home/leon/Documents/researchProjects/wikidata/data/totalChanges.csv")
    val fin = new FileInputStream(file)
    new BufferedReader(new InputStreamReader(fin))
  }

  def getFullWikidataSparkCompatibleFile() = new File("/home/leon/Documents/researchProjects/wikidata/data/totalChangesSparkCompatible.csv")

  def getBZ2CompressedOutputStream(f:File) ={
    new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, new FileOutputStream(f));
  }

  def getSettlementsFile = new File("/home/leon/Documents/researchProjects/wikidata/data/settlementsNew.csv")

  def getSettlementsCategoryMapFile = new File(processedResultDir() + "settlementsCategoryMap.obj")

  def getFullCategoryMapFile = new File(processedResultDir() + "fullCategoryMap.obj")
}
