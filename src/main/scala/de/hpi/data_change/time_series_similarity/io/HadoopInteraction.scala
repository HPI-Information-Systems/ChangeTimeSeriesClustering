package de.hpi.data_change.time_series_similarity.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HadoopInteraction(userName:String,hdfsUrI:String) extends Serializable{
  def exists(path: String): Boolean = {
    return fs.exists(new Path(path))
  }


  System.setProperty("HADOOP_USER_NAME", userName)
  val conf = new Configuration()
  conf.set("fs.defaultFS", hdfsUrI) //TODO: make this a parameter?
  val fs = FileSystem.get(conf)

  def this(){
    this("leon.bornemann","hdfs://mut:8020")
  }

  def writeToFile(content:String,filePath:String): Unit ={
    val os = fs.create(new Path(filePath))
    os.write(content.getBytes())
    os.close()
  }

  def appendLineToFile(line:String,filePath:String): Unit ={
    val appender = fs.append(new Path(filePath))
    appender.writeBytes(line)
    appender.writeChar('\n')
    appender.close()
  }

  def appendLineToCsv(results: Seq[String], filePath: String) = {
    val line= results.reduce( (s1,s2) => s1+","+s2)
    if(fs.exists(new Path(filePath))){
      appendLineToFile(line,filePath)
    } else{
      writeToFile(line +"\n",filePath)
    }
  }

}
