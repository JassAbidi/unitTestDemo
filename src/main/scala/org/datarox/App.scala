package org.datarox

import org.apache.spark.sql.SparkSession
object App {

  def main(args : Array[String]) {
    // declare sparksession
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    import spark.implicits._
    val df1 = (1 to 100).toDF("id")
    val df2 = (10 to 50).toDF("id")
    df1.localCheckpoint().filter($"id" < 50 ).rdd.collect
  }

}
