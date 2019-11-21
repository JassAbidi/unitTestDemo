package org.datarox

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Rules {

  def filterOnAge(df: DataFrame, age: Int): DataFrame = {
    df.filter(col("age") <= age)
  }

  def tagAsCurrent(df: DataFrame): DataFrame = {
    df.withColumn("current", when(col("endDate") isNull, true).otherwise(false))
  }

/*  def selectUpdateMap(): Map[String, String]={
    when()
  }*/


}
