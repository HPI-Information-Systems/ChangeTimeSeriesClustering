package de.hpi.data_change.time_series_similarity.io

import java.io._

import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.CompressorStreamFactory


class DataIO {

}

object DataIO{

  def getOriginalFullWikidataFileStream = {
    val file = new File("/home/leon/Documents/data/wikidata/20120323-en-updates_new_full.csv.bz2")
    val fin = new FileInputStream(file)
    val bis = new BufferedInputStream(fin)
    val input = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2,bis)
    new BufferedReader(new InputStreamReader(input))
  }

  def getWikidataRepairedFile() = new File("/home/leon/Documents/data/wikidata/20120323-en-updates_new_full_repaired.csv.bz2")

  def getWikidataRepairedFileWithoutValue() = new File("/home/leon/Documents/data/wikidata/20120323-en-updates_new_full_repaired_without_Value.csv.bz2")

  def getBZ2CompressedOutputStream(f:File) ={
    new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, new FileOutputStream(f));
  }

  def getSettlementsFile = new File("/home/leon/Documents/data/wikidata/settlements/dump.csv")

}