package org.datarox

import io.delta.tables.DeltaTable
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

case class Customer(customerId: Long,
                    address: String,
                    current: Boolean,
                    effectiveDate: String,
                    endDate: String)

case class Update(customerId: Long, address: String, effectiveDate: String)

class DeltaDMLSpec extends FlatSpec with Matchers with GivenWhenThen with SharedSparkSession {
  val customerTable = Seq(
    Customer(1, "address 1", false, "2019-01-01", "2019-01-02"),
    Customer(1, "address 2", true, "2019-01-02", null),
    Customer(2, "address 2", true, "2019-01-02", null),
    Customer(3, "address 3", true, "2019-01-03", null)
  )

  val customerTable2 = Seq(
    Customer(1, "address 1", false, "2019-01-01", "2019-01-02"),
    Customer(1, "address 2", false, "2019-01-02", "2019-01-05"),
    Customer(1, "address 4", true, "2019-01-05", null),
    Customer(2, "address 2", true, "2019-01-02", null),
    Customer(3, "address 3", true, "2019-01-03", null),
    Customer(0, "address 0", true, "2019-01-02", null)
  )

  val corrections = Seq(
    // < effective Date
    Update(0, "address 0", "2019-01-01"),
    //in [effective date, endDate]
    Update(1, "address 5", "2019-01-03"),
    // > endDate
    Update(2, "address 6", "2019-01-06"),
    // == effective Date
    Update(3, "address 3", "2019-01-06"),
    // new id
    Update(4, "address 10", "2019-01-10")
  )

  val updates = Seq(
    Update(1, "address 4", "2019-01-05"),
    Update(2, "address 2", "2019-01-04"),
    Update(4, "address 3", "2019-04-01")
  )


  val customerTableAfterCorrection = Seq(
    Customer(1, "address 1", false, "2019-01-01", "2019-01-01"),
    Customer(1, "address 4", false, "2019-01-03", "2019-01-04"),
    Customer(1, "address 2", true, "2019-01-04", null),
    Customer(2, "address 2", true, "2019-01-02", null),
    Customer(3, "address 3", true, "2019-01-03", null)
  )


  override def beforeAll(): Unit = {
    super.beforeAll()
    val spark = sparkSession
    import spark.implicits._
    customerTable.toDF()
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\customerTable")
    customerTable2.toDF()
      .write
      .format("delta")
      .mode("overwrite")
      .save("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\customerTable2")
  }

  "data correction" should " " in {
    val spark = sparkSession
    import spark.implicits._
    val updateCondition =
      """
        |((stagingUpdates.effectiveDate > customers.effectiveDate AND (stagingUpdates.effectiveDate < customers.endDate OR customers.endDate is NULL))
        | AND customers.address <> stagingUpdates.address)
      """.stripMargin


    val insertCondition =
      """
        |((corrections.effectiveDate > customers.effectiveDate AND (corrections.effectiveDate < customers.endDate OR customers.endDate is NULL))
        | AND customers.address <> corrections.address) OR
        | (corrections.effectiveDate < customers.effectiveDate AND customers.address <> corrections.address)
      """.stripMargin
    val customerTable = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\customerTable2")

    val customerstoInsert = corrections.toDF
      .as("corrections")
      .join(customerTable.toDF.as("customers"), Seq("customerId"))
      .where(insertCondition)
      .selectExpr("NULL as mergeKey ", "customerId", "corrections.address", "corrections.effectiveDate", "customers.endDate")
      .transform(Rules.tagAsCurrent)

    val stagingUpdates = customerstoInsert
      .union(corrections.toDF().as("corrections").selectExpr("corrections.customerId as mergeKey", "corrections.*", "NULL as endDate", "NULL as current"))

    val cond1 =
    """
      |CASE
      |      WHEN `customers.address` > 1 THEN false
      |      ELSE true
      |    END
    """.stripMargin

    DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\customerTable2")
      .as("customers")
      .merge(stagingUpdates.as("stagingUpdates"), "customers.customerId = mergeKey")
      .whenMatched(updateCondition)
      .updateExpr(Map(
        "effectiveDate" -> "",
        "endDate" -> "stagingUpdates.effectiveDate",
        "current" -> cond1
      ))
      .whenMatched("(stagingUpdates.effectiveDate < customers.effectiveDate AND customers.address = stagingUpdates.address)")
      .updateExpr(Map(
        "effectiveDate" -> "stagingUpdates.effectiveDate"
      ))
      .whenNotMatched()
      .insertExpr(Map(
        "customerId" -> "stagingUpdates.customerId",
        "address" -> "stagingUpdates.address",
        "current" -> "stagingUpdates.current",
        "effectiveDate" -> "stagingUpdates.effectiveDate",
        "endDate" -> "stagingUpdates.endDate"
      ))
      .execute()

    customerTable
      .as("customers")
      .toDF
      .show



  }


  "(stagingUpdates.effectiveDate > customers.effectiveDate AND (stagingUpdates.effectiveDate < customers.endDate OR customers.endDate is NULL)) AND customers.address <> stagingUpdates.address"

  "((corrections.effectiveDate > customers.effectiveDate AND (corrections.effectiveDate < customers.endDate OR customers.endDate is NULL)) AND customers.address <> corrections.address) OR (stagingUpdates.effectiveDate < customers.effectiveDate AND customers.address <> corrections.address)"


  "" should "" in {
    val spark = sparkSession
    import spark.implicits._

    val customerTable = DeltaTable
      .forPath(spark, "C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\customerTable")
    val updatesDf = updates.toDF()

    val newCustomers = updatesDf
      .as("updates")
      .join(customerTable.toDF.as("customers").select($"customerId".as("leftCustomerId")), $"leftCustomerId" === $"customerId", "left")
      .filter($"leftCustomerId".isNull)
      .select("updates.*")

    val existingCustomersWithNewAddresses = updatesDf
      .as("updates")
      .join(
        customerTable.toDF.as("customers").select($"customerId", $"address".as("old_address"), $"current"),
        Seq("customerId"))
      .filter("customers.current = true AND old_address <> updates.address")
      .selectExpr("updates.*")

    val newAddressesToInsert = newCustomers
      .union(existingCustomersWithNewAddresses)
      .selectExpr("NULL as mergeKey", "updates.*")

    val stagedUpdates = newAddressesToInsert
      .union(existingCustomersWithNewAddresses.as("updates").selectExpr("customerId as mergeKey", "updates.*"))

    customerTable
      .as("customers")
      .merge(stagedUpdates.as("staged_updates"),
        "customers.customerId = mergeKey")
      .whenMatched("customers.current = true AND customers.address <> staged_updates.address")
      .updateExpr(Map(
        "current" -> "false",
        "endDate" -> "staged_updates.effectiveDate"))
      .whenNotMatched()
      .insertExpr(Map(
        "customerid" -> "staged_updates.customerId",
        "address" -> "staged_updates.address",
        "current" -> "true",
        "effectiveDate" -> "staged_updates.effectiveDate",
        "endDate" -> "null"))
      .execute()

    customerTable
      .as("customers")
      .toDF
      .show

  }

  "case when in select expression" should " " in {

    val spark = sparkSession
    import spark.implicits._
    ""
    val selectExp =
      """
        |CASE
        |      WHEN `customerId` > 1 then `customerId`
        |      END
      """.stripMargin

    val updatesDf =
      updates.toDF()
      .selectExpr(selectExp)
      .show




  }
}
