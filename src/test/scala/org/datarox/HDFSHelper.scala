package org.datarox

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


case class HDFSHelper[T](uri: String) extends Serializable {
  val conf = new Configuration()
  conf.set("fs.defaultFS", uri)
  val hdfs: FileSystem = FileSystem.get(conf)

  def createDir(dirPath: String): Boolean = {
    hdfs.mkdirs(new Path(dirPath))
  }
}
