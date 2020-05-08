package org.datarox

import io.delta.tables.DeltaTable
import org.datarox.Rules._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.scalatest._

case class Schema1(id: Int, col1: String, col2: String)

case class Schema2(id: Int, col1: String, col2: String, col3: String)

class MergeSchemaEvolutionSpec extends FlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .appName("AeroportTest")
    .master("local[*]")
    .getOrCreate()

  val data1 = Seq(
    Schema1(1, "a", "1"),
    Schema1(2, "a", "5"),
    Schema1(3, "b", "5")
  )

  val data2 = Seq(
    Schema2(1, "b", "4.0", "a"),
    Schema2(2, "c", "5.0", "b"),
    Schema2(4, "b", "5.0", "c")
  )


  "merge schema" should "" in {
    import spark.implicits._

    val df1 = data1.toDF
    val df2 = data2.toDF

    df1.write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")

    val table = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")


    table.as("table")
      .merge(df2.as("delta"), "table.id == delta.id")
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()


    df1.show
    spark
      .read
      .format("delta")
      .load("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")
      .show()

    val updatedTable = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")

  }

  " schema evolution " should "" in {

    import spark.implicits._

    val df2 = data2.toDF
    val df1 = data1.toDF


    df2
      .write
      .format("delta")
      .mode("append")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")


    df1.write
      .option("mergeSchema", "true")
      .option("overwriteSchema", "true")
      .format("delta")
      .mode("append")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")


    val updatedTable = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")

    updatedTable.toDF.show()
  }


}
