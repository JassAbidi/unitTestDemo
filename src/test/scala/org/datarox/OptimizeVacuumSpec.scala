package org.datarox

import java.net.URI

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.datarox.Rules._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.scalatest._

case class Schema0(id: Int, col1: String, col2: String)

//case class Schema22(id: Int, col1: String, col2: String, col3: String)

class OptimizeVacuumSpec extends FlatSpec with Matchers with GivenWhenThen {

  val spark = SparkSession
    .builder()
    .appName("vaccum-test")
    .master("local[*]")
    .getOrCreate()

  val data1 = Seq(
    Schema0(1, "a", "1"),
    Schema0(2, "a", "5"),
    Schema0(3, "b", "5")
  )

  val data2 = Seq(
    Schema2(1, "b", "4.0", "a"),
    Schema2(2, "c", "5.0", "b"),
    Schema2(4, "b", "5.0", "c")
  )


  "test vacuum" should "" in {
    import spark.implicits._

    val df1 = data1.toDF
    val df2 = data2.toDF

   /* df1.write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")
*/
    val table = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")

    /*

        table.as("table")
          .merge(df2.as("delta"), "table.id == delta.id")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
    */

    /*df1.show
    spark
      .read
      .format("delta")
      .load("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")
      .show()
*/
    val updatedTable = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\table")

    updatedTable.vacuum()

    Thread.sleep(1000000000)
  }

  "test serializable" should "" in {
    import spark.implicits._
    def stringToPath(path: String): Path = new Path(new URI(path))
    val ds = Seq("/a/b/c", "a/b/d").toDS()

    val res = ds.mapPartitions{files =>
      val paths = files.map(f =>new Path(new URI(f)))
      Iterator(1)
    }.reduce(_+_)
    println("**********")
    println(res)
    println("**********")
  }


}
