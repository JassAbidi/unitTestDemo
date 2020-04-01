package org.datarox

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Rules {

  def filterOnAge(df: DataFrame, age: Int): DataFrame = {
    df.filter(col("age") <= age)
  }

  def tagAsCurrent(df: DataFrame): DataFrame = {
    df.withColumn("current", when(col("endDate") isNull, true).otherwise(false))
  }

  def skipHeader (dataframe: DataFrame, headerFirstColumn: String, firstColumnName: String)(implicit  spark :SparkSession ) :DataFrame = {
    import spark.implicits._
    dataframe.filter(col(firstColumnName) =!= headerFirstColumn)
  }

/*  def selectUpdateMap(): Map[String, String]={
    when()
  }*/

}
