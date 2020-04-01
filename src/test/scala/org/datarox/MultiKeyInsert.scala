package org.datarox

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}


case class Record(customerId: Long, name: String, salary: Int, startDate: String, endDate: String)

case class DeltaRow(customerId: Long, name: String, salary: Int, date: String)

class MultiKeyInsertSpec extends FlatSpec with Matchers with GivenWhenThen with SharedSparkSession {

  val history = Seq(
    Record(4, "Nidhal", 4000, "06/01/2016", "08/01/2016"),
    Record(4, "Nidhal", 5000, "08/01/2016", null),
    Record(1, "John", 1000, "01/01/2016", "03/01/2016"),
    Record(1, "John", 2000, "03/01/2016", null),
    Record(1, "John", 2000, "03/01/2016", null),
    Record(2, "Jasser", 2000, "02/01/2016", "04/01/2016"),

    Record(2, "Jasser", 3000, "04/01/2016", null),

    Record(3, "Amine", 3000, "05/01/2016", "06/01/2016"),
    Record(3, "Amine", 4000, "06/01/2016", null)
  )

  val delta = Seq(
    DeltaRow(2, "Jasser", 4000, "05/01/2016"),
    DeltaRow(1, "John", 4000, "05/01/2016"),
    DeltaRow(2, "Jasser", 2500, "03/01/2016"),
    DeltaRow(4, "Nidhal", 45000, "07/01/2016")
  )


  "" should "update the history with the new delta data" in {
    val spark = sparkSession
    import spark.implicits._

    def updateHistory(history: DataFrame)(delta: DataFrame): DataFrame = {
      val window = Window.partitionBy('customerId).orderBy('startDate)
      val endDateCol = lead(col("startDate"), 1).over(window)

      history
        .union(delta)
        .withColumn("endDate", endDateCol)
    }

    val historyDf = history.toDF
    val deltaDf = delta.toDF
      .withColumnRenamed("date", "startDate")

      historyDf
        .drop('endDate)
        .transform(updateHistory(deltaDf)).show
      Thread.sleep(1000000000)
  }
}
