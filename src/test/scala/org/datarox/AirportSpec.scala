package org.datarox

import org.datarox.Rules._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.scalatest._

case class Person(firstName: String, age: String)
class AirportSpec extends FlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .appName("AeroportTest")
    .master("local[*]")
    .getOrCreate()



  val testData = Seq(Person("name", "age"),
    Person("ahmed", "26"),
    Person("Jacer", "28"))

  val dataWithoutHeader = Seq(
    Person("ahmed", "26"),
    Person("Jacer", "28"))
  "func" should "" in {


  /*  val loadedFile = spark.read.format("csv")
      .option("delimiter", ",")
      //.option("header", "TRUE")
      // .option("inferSchema", "TRUE")
      .load("C:\\Users\\EliteBook\\Desktop\\books\\687973024_T_ONTIME_REPORTING.csv")
  */
    import spark.implicits._
    val personDF = testData.toDF()
    val dFWithoutHeader = skipHeader(personDF, "name", "firstName")

    dFWithoutHeader.collect() should contain theSameElementsAs dataWithoutHeader.toDF().collect()





    val loadedFile = spark.read.format("csv")
      .option("delimiter", ",")
     .option("header", "TRUE")
      .option("inferSchema", "TRUE")
      .load("C:\\Users\\EliteBook\\Desktop\\books\\687973024_T_ONTIME_REPORTING.csv")

    val window = Window.partitionBy()
    loadedFile.groupBy('DEST_AIRPORT_ID).count().orderBy('count.desc).show
  }


}
