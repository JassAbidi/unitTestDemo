package org.datarox

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

case class CustomerReferencial(customerId: Long, address: String, cdcTimestamp: String)

case class UpdateFinal(customerId: Long, address: String, cdcTimestamp: String)

case class UpdateStaging(flag: String, customerId: Long, address: String, cdcTimestamp: String)

case class CDCRecord(flag: String, customerId: Long, address: String, cdcTimestamp: String)

class CDCSpec extends FlatSpec with Matchers with GivenWhenThen with SharedSparkSession {

  val customerTable = Seq(
    CustomerReferencial(1, "address 1", "2019-01-02"),
    CustomerReferencial(2, "address 2", "2019-01-02"),
    CustomerReferencial(3, "address 3", "2019-01-03")
  )

  val updateFinal = Seq(
    UpdateFinal(1, "address 1", "2019-01-02"),
    UpdateFinal(2, "address 2", "2019-01-02"),
    UpdateFinal(3, "address 3", "2019-01-03")
  )

  val updateStaging = Seq(
    UpdateStaging("I", 1, "address 1", "2019-01-02"),
    UpdateStaging("I", 2, "address 2", "2019-01-02"),
    UpdateStaging("I", 3, "address 3", "2019-01-03")
  )

  val CDCRecords = Seq(
    CDCRecord("I", 4, "address 4", "2019-01-04"),
    CDCRecord("U", 4, "address 5", "2019-01-05"),
    CDCRecord("D", 4, "address 5", "2019-01-06"),
    CDCRecord("U", 2, "address 6", "2019-01-04"),
    CDCRecord("D", 3, "address 3", "2019-01-04"),
    CDCRecord("I", 5, "address 5", "2019-01-05")
  )


  override def beforeAll(): Unit = {
    super.beforeAll()
    val spark = sparkSession
    import spark.implicits._

    customerTable.toDF()
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")

    updateFinal.toDF()
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateFinal")

    updateStaging.toDF()
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateStaging")

  }

  "CDC implementation version 1" should " " in {
    val spark = sparkSession
    import spark.implicits._

    val window = Window.partitionBy($"customerId").orderBy($"cdcTimestamp".desc)

    val cdc = CDCRecords.toDF
    cdc
      .write
      .format("delta")
      .mode("append")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateStaging")


    val stagingUpdates = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateStaging")
      .toDF
      .withColumn("rank", rank over window)
      .filter($"rank" === 1 && $"flag" =!= "D")
      .select($"customerId", $"address", $"cdcTimestamp")
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateFinal")


    val customers = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")
      .toDF
      .as("customers")

    val finalUpdates = DeltaTable
      .forPath("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateFinal")
      .toDF
      .as("updates")
      .withColumn("flag", lit("I"))


    val recordsToDelete = customers
      .join(finalUpdates, Seq("customerId"), "left")
      .where("updates.customerId is NULL")
      .selectExpr("customers.*")
      .withColumn("flag", lit("D"))

    val updates = finalUpdates.union(recordsToDelete)

    DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")
      .as("customers")
      .merge(updates.as("updates"), "customers.customerId = updates.customerId")
      .whenMatched("customers.address <> updates.address")
      .updateExpr(Map(
        "address" -> "updates.address",
        "cdcTimestamp" -> "updates.cdcTimestamp"
      ))
      .whenMatched("customers.address = updates.address AND flag = 'D' ")
      .delete()
      .whenNotMatched()
      .insertAll()
      .execute()


    DeltaTable
      .forPath("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")
      .toDF
      .show
  }

  "optimized CDC based on version 1" should "" in {
    val spark = sparkSession
    import spark.implicits._

    def getREcordsToDelete(cdcRecords: DataFrame): DataFrame = {

      val recordsToInsert = cdcRecords.filter($"flag" === "I").select("customerId")
      val recordsToDelete = cdcRecords.filter($"flag" === "D")

      recordsToDelete
        .as("toDelete")
        .join(recordsToInsert.as("toInsert"), Seq("customerId"), "left")
        .where("toInsert.customerId is NULL")
        .selectExpr("toDelete.customerId", "toDelete.address", "toDelete.cdcTimestamp", "toDelete.flag")
      // Potential SPARK BUG in column order when using *
      //.selectExpr("toDelete.*")
    }

    val window = Window.partitionBy($"customerId").orderBy($"cdcTimestamp".desc)

    val cdc = CDCRecords.toDF
    cdc
      .write
      .format("delta")
      .mode("append")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateStaging")

    val stagingUpdates = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateStaging")
      .toDF
      .withColumn("rank", rank over window)
      .filter($"rank" === 1 && $"flag" =!= "D")
      .select($"customerId", $"address", $"cdcTimestamp")
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateFinal")


    val customers = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")
      .toDF
      .as("customers")

    val finalUpdates = DeltaTable
      .forPath("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\updateFinal")
      .toDF
      .as("updates")
      .withColumn("flag", lit("I"))

    val recordsToDelete = getREcordsToDelete(CDCRecords.toDF)


    val updates = finalUpdates.union(recordsToDelete)

    DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")
      .as("customers")
      .merge(updates.as("updates"), "customers.customerId = updates.customerId")
      .whenMatched("customers.address <> updates.address")
      .updateExpr(Map(
        "address" -> "updates.address",
        "cdcTimestamp" -> "updates.cdcTimestamp"
      ))
      .whenMatched("customers.address = updates.address AND flag = 'D' ")
      .delete()
      .whenNotMatched()
      .insertAll()
      .execute()


    DeltaTable
      .forPath("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\ReferentielCustomerTable")
      .toDF
      .show


  }

  val customers = Seq(
    CustomerReferencial(1, "jasser", "2019-01-02"),
    CustomerReferencial(2, "nidhal", "2019-01-02"),
    CustomerReferencial(3, "ahmed", "2019-01-03")
  )

  val cdcPath = "pathToCsv"

  "cdc implemetation " should "read cdc from a csv file and apply them to Hive table" in {





  }


}
