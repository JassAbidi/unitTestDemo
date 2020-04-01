package org.datarox


import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest._
import tables.{OptimizedTable, Table}

case class PersonBis(firstName: String, age: String, value: Int)

class FileIndexSpec extends FlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .appName("AeroportTest")
    .master("local[*]")
    .getOrCreate()

  val testData = Seq(
    PersonBis("ahmed", "26", 2),
    PersonBis("nidhal", "26", 2),
    PersonBis("Jacer", "28", 3),
    PersonBis("ahmed", "28", 2)
  )


  "delete physically " should "" in {
    import spark.implicits._

    val file = "///temp"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)
    fs.delete(path, true)
    fs.mkdirs(path)

    val personDF = testData.toDF()
    personDF.repartition(col("age")).write.mode("overwrite").partitionBy("age").parquet(path + "/person")

    val table = Table.fromPath(path + "/person")

    //table.delete(/*col("age") === 26 and*/ col("firstName") === "ahmed")
    table.delete(col("value") === 3 and col("age") === "28")
    spark.read.parquet(path + "/person").show
  }


  "delete logically " should "" in {
    import spark.implicits._

    val file = "///temp"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)
    fs.delete(path, true)
    fs.mkdirs(path)

    val personDF = testData.toDF()
    personDF.repartition(col("age")).write.mode("overwrite").partitionBy("age").parquet(path + "/person")

    val table = Table.fromPath(path + "/person")

    //table.delete(/*col("age") === 26 and*/ col("firstName") === "ahmed")
    table.deleteLogically(col("value") === 3 /*and col("firstName") === "ahmed"*/)
    spark.read.parquet(path + "/person").show(false)
    spark.read.parquet(path + "/archive/person").show(false)
  }

  "revert" should "" in {
    import spark.implicits._

    val file = "/temp"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)
    fs.delete(path, true)
    fs.mkdirs(path)

    val personDF = testData.toDF()
    personDF.repartition(col("age")).write.mode("overwrite").partitionBy("age").parquet(path + "/person")

    val table = Table.fromPath(path + "/person")

    //table.delete(/*col("age") === 26 and*/ col("firstName") === "ahmed")
    table.deleteLogically(col("firstName") === "ahmed" /*and col("firstName") === "ahmed"*/)
    spark.read.parquet(path + "/person").show(false)
    table.revert(48)
    spark.read.parquet(path + "/person").show(false)
    //spark.read.parquet(path + "/person").show(false)
    //spark.read.parquet(path + "/archive/person").show(false)
  }

  "delete on table " should "" in {
    import spark.implicits._

    val file = "/temp"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)
    fs.delete(path, true)
    fs.mkdirs(path)
    val personDF = testData.toDF()
    //personDF.repartition(col("age")).write.mode("overwrite").partitionBy("age").parquet(path + "/person")
    spark.sql("use default")
    personDF.repartition(col("age")).write
      .mode("overwrite")
      .partitionBy("age")
      .saveAsTable("default.person")


    val table = Table.fromName("default.person")
    table.delete(" age == 28 ")

    Table.fromName("default.person").toDF().show

    spark.sql("drop table default.person")
    fs.delete(table.basePath, true)
  }

  "delete logically on table " should "" in {
    import spark.implicits._

    val file = "/temp"
    val warehouse = "file:/C:/Users/EliteBook/IdeaProjects/unittestdemo/spark-warehouse/"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)

    spark.sql("drop table if exists default.person")
    fs.delete(path, true)
    fs.delete(new Path(warehouse + "person"), true)
    fs.delete(new Path(warehouse + "archive/person"), true)
    fs.mkdirs(path)

    val personDF = testData.toDF()

    spark.sql("use default")
    personDF.repartition(col("age")).write
      .mode("overwrite")
      .partitionBy("age")
      .saveAsTable("default.person")

    val table = Table.fromName("default.person")
    //    table.deleteLogically(" age  > 26 ")
    table.deleteLogically(col("firstName") === "ahmed")
    Table.fromName("default.person").toDF().show

    //table.revert(1)
    //table.revert(1)

    // Table.fromName("default.person").toDF().show
  }


  "vacuum on table" should "" in {
    import spark.implicits._

    val file = "/temp"
    val warehouse = "file:/C:/Users/EliteBook/IdeaProjects/unittestdemo/spark-warehouse/"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)

    spark.sql("drop table if exists default.person")
    fs.delete(path, true)
    fs.delete(new Path(warehouse + "person"), true)
    fs.delete(new Path(warehouse + "archive/person"), true)
    fs.mkdirs(path)

    val personDF = testData.toDF()

    spark.sql("use default")
    personDF.repartition(col("age")).write
      .mode("overwrite")
      .partitionBy("age")
      .saveAsTable("default.person")

    val table = Table.fromName("default.person")

    table.deleteLogically(" age == 28 ")

    table.vacuum(1)

    //table.vacuumAll()

  }


  "delete all data" should "" in {
    import spark.implicits._

    val file = "/temp"
    val warehouse = "file:/C:/Users/EliteBook/IdeaProjects/unittestdemo/spark-warehouse/"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)

    spark.sql("drop table if exists default.person")
    fs.delete(path, true)
    fs.delete(new Path(warehouse + "person"), true)
    fs.delete(new Path(warehouse + "archive/person"), true)
    fs.mkdirs(path)

    val personDF = testData.toDF()

    spark.sql("use default")
    personDF.repartition(col("age")).write
      .mode("overwrite")
      .partitionBy("age")
      .saveAsTable("default.person")

    val table = Table.fromName("default.person")

    table.deleteLogically()

    Table.fromName("default.person").toDF().show

    table.revert(1)

    Table.fromName("default.person").toDF().show
  }


  "test prune file index based on partitions " should "" in {
    import spark.implicits._

    val file = "/temp"
    val warehouse = "file:/C:/Users/EliteBook/IdeaProjects/unittestdemo/spark-warehouse/"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)

    spark.sql("drop table if exists default.person")
    fs.delete(path, true)
    fs.delete(new Path(warehouse + "person"), true)
    fs.delete(new Path(warehouse + "archive/person"), true)
    fs.mkdirs(path)

    val personDF = testData.toDF()

    spark.sql("use default")
    personDF.repartition(col("age")).write
      .mode("overwrite")
      .partitionBy("age")
      .saveAsTable("default.person")

    /*

        val table = OptimizedTable.fromName("default.person")

        table.delete(col("age") === "20")
        //table.delete(col("age") === "28")
        table.toDF().show
    */

    val table = Table.fromName("default.person")
    // table.delete( col("value") === 3)
    table.delete(col("age") === "28" or col("firstName").isin("nidhal", "Jacer"))
    table.toDF().show


  }

  "delete from non partitionned table " should "" in {

    import spark.implicits._

    val file = "/temp"
    val warehouse = "file:/C:/Users/EliteBook/IdeaProjects/unittestdemo/spark-warehouse/"
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)

    spark.sql("drop table if exists default.person")
    fs.delete(path, true)
    fs.delete(new Path(warehouse + "person"), true)
    fs.delete(new Path(warehouse + "archive/person"), true)
    fs.mkdirs(path)

    val personDF = testData.toDF()

    spark.sql("use default")
    personDF /*.repartition(col("age"))*/
      .write
      .mode("overwrite")
      /*.partitionBy("age")*/
      .saveAsTable("default.person")


    /*
            val table = OptimizedTable.fromName("default.person")

            table.delete(col("age") === "20")
            //table.delete(col("age") === "28")
            table.toDF().show

    */


    OptimizedTable.refreshTable("default", "person").foreach(println)
    println(OptimizedTable.refreshTable("default", "person").isEmpty)
  }

}



