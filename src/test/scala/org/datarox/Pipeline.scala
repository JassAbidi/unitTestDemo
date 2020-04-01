package org.datarox

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

case class Etl(reader: (String, Map[String, String]) => DataFrame,
                    transformer: Seq[DataFrame => DataFrame],
                    writer: DataFrame => Unit) {

  def csvReader(path: String, options: Map[String, String])(spark: SparkSession): DataFrame = {
    spark.read.csv(path)
  }

  def run(path: String, readerOptions: Map[String, String], transformerOptions: Map[String, String]) = {
    writer(reader(path, readerOptions))//.transform(transformer))
  }

  def func1(df: DataFrame): DataFrame = ???

  def func2(df: DataFrame): DataFrame = ???

  def func3(df: DataFrame): DataFrame = ???

  def func4(df: DataFrame): DataFrame = ???

  val pipeline : Seq[DataFrame => DataFrame] = Seq(func1, func3, func4)

  def runPipeline(df: DataFrame, pipeline : Seq[DataFrame => DataFrame]): DataFrame = {
    pipeline.foldLeft(df){
      (finalDf, transformation) => finalDf.transform(transformation)
    }
  }

}





