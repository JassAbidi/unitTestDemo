package org.datarox

import org.apache.spark.sql.SparkSession

import org.datarox.Rules._
import org.scalatest._

case class User(nom: String, prenom: String, age: Int)

class RulesExamplesSpec extends FlatSpec with Matchers with GivenWhenThen {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  val users = Seq(
    User("BEN JEBARA", "Ahmed", 25),
    User("ABIDI", "Jasser", 27),
    User("SADAAOUI", "Nidhal", 21),
    User("MASMOUDI", "Zaineb", 23)
  )

  val usersUnder23 = Seq(
    User("MASMOUDI", "Zaineb", 23),
    User("SADAAOUI", "Nidhal", 21)
  )

/*
  "filterOnAge" should "filter dataframe on age" in {
    import spark.implicits._
    Given("input dataframe and age")
    val age = 23
    val inputDataframe = users.toDF

    When("filterOnAge is invoked")
    val result = filterOnAge(inputDataframe, age)

    Then("users with age under 23 should be returned")
    result.collect() should contain theSameElementsAs usersUnder23.toDF().collect()
  }
*/

  "" should "" in {

    val tempDir = "C:\\Users\\EliteBook\\Documents\\output\\deltaTab2"
    (1 to 22).foreach(x => spark.range(100).write.mode("append").format("delta").save(tempDir))

    val df = spark.read.format("parquet")
      .load("C:\\Users\\EliteBook\\Documents\\output\\deltaTab\\_delta_log\\00000000000000000020.checkpoint.parquet")
    df.show(100, false)
    df.count()

  }


  "partition" should "" in {
    import spark.implicits._

    //spark.sql("create table tab3 (id Int) partitioned by (part Int) stored as PARQUET")


    /* val data = (1 to  1000000).toDF("id")
        .withColumn("part", 'id % 2)
        .write
        .mode("overwrite")
        .partitionBy("part")
        .save("C:\\Users\\EliteBook\\Documents\\output\\tab2")

      (1 to  1000000).toDF("id")
        .withColumn("part", 'id % 3)
        .repartition('part)
        .write
        .mode("overwrite")
        .save("C:\\Users\\EliteBook\\Documents\\output\\tab2")
  */

    // use
    (1 to 1000000).toDF("id")
      .withColumn("part", 'id % 3)
      .repartition('part)
      .write
      .mode("append")
      .insertInto("tab3")

    Thread.sleep(1000000000)
  }


  "flatten json tested " should " " in {

    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    def explodeArrayType(df: DataFrame, columnToExplode: String): DataFrame = {
      val allColumns = df.schema.fields.map(field => field.name)
      val allColumnsMinusColumnToExplode = allColumns.filter(_ != columnToExplode).map(column => col(column))
      df.select(allColumnsMinusColumnToExplode ++ Array(explode(col(columnToExplode)).as(columnToExplode)): _*)
    }


    def explodeStructType(df: DataFrame, columnToExplode: String, nestedColumns: StructType): DataFrame = {
      val allColumns = df.schema.fields.map(field => field.name)
      val allColumnsMinusColumnToExplode = allColumns.filter(_ != columnToExplode).map(column => col(column))
      val explodedColumns = nestedColumns.fieldNames.map { field =>
        val column = columnToExplode + "." + field
        col(column).as(column.replace(".", "_"))
      }
      df.select(allColumnsMinusColumnToExplode ++ explodedColumns: _*)
    }

    def flatten(df: DataFrame): DataFrame = {
      val fields = df.schema.fields
      for (i <- 0 to fields.length - 1) {
        val column = fields(i)
        column.dataType match {
          case _: ArrayType => return flatten(explodeArrayType(df, column.name))
          case structType: StructType => return flatten(explodeStructType(df, column.name, structType))
          case _ =>
        }
      }
      df
    }

    def flattenDataframe(df: DataFrame): DataFrame = {

      val fields = df.schema.fields
      val fieldNames = fields.map(x => x.name)
      val length = fields.length

      for (i <- 0 to fields.length - 1) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          case arrayType: ArrayType =>
            val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
            // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
            val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
            return flattenDataframe(explodedDf)
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
            val explodedf = df.select(renamedcols: _*)
            return flattenDataframe(explodedf)
          case _ => df
        }
      }
      df
    }

    val data = spark.read.option("multiline", "true").json("C:\\Users\\EliteBook\\Desktop\\BIG DATA\\jasser\\databricks\\data\\data.json")


    // data.select(col("meta.*"), explode(col("records"))).show

    flatten(data).show
  }


  "window" should "" in {
    import org.apache.spark.sql.functions.window
    import org.apache.spark.sql.types._
    import spark.implicits._
    val jsonSchema = new StructType().add("time", TimestampType).add("action", StringType)

    val data = spark.read.schema(jsonSchema).json("C:\\Users\\EliteBook\\Desktop\\BIG DATA\\jasser\\databricks\\events")

    data.groupBy('action, window('time, "1 hour")).count.show(false)


  }

  " streaming example " should "" in {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._

    val inputPath = "C:\\Users\\EliteBook\\Desktop\\BIG DATA\\jasser\\databricks\\events"
    val jsonSchema = new StructType().add("time", TimestampType).add("action", StringType)

    val inputDF =
      spark
        .read
        .schema(jsonSchema)
        .json(inputPath)
        .show

    val streamingInputDF =
      spark
        .readStream
        .schema(jsonSchema)
        .option("maxFilesPerTrigger", 1)
        .json(inputPath)

    val streamingCountsDF =
      streamingInputDF
        .groupBy($"action", window($"time", "1 hour", "10 minutes"))
        .count()
        .writeStream
        .format("memory") // memory = store in-memory table (for testing only in Spark 2.0)
        .queryName("counts") // counts = name of the in-memory table
        .outputMode("complete") // complete = all the counts should be in the table
        .start()

  }

  "test foldLeft" should "" in {
    val nums = List(1, 2, 3, 4)

    val sum = nums.foldLeft(0) {
      (acc, num) => acc + num
    }


    "" should "" in {


    }

    "test select" should "" in {
      import org.apache.spark.sql.functions.col
      import spark.implicits._
      Given("input dataframe and age")
      val age = 23
      val inputDataframe = users.toDF
        .select(col("name"))
        .select($"name")
        .select('name, 'age)
        .selectExpr("name", "age")
    }


  }
}