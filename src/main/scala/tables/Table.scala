package tables

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Table(df: DataFrame, location: String) extends TableOperations {

  val ARCHIVE = "/archive/"

  def as(alias: String): DataFrame = df.as(alias)

  def toDF(): DataFrame = df

  def basePath: Path = new Path(location)

  def archivePath: Path = {
    new Path(basePath.getParent.toString + ARCHIVE + basePath.getName)
  }

  def delete(condition: String): Unit = {
    delete(expr(condition))
  }

  def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))

  }

  def delete(): Unit = {
    executeDelete(None)
  }

  def deleteLogically(condition: String): Unit = {
    deleteLogically(expr(condition))
  }

  def deleteLogically(condition: Column): Unit = {
    executeDeleteLogically(Some(condition.expr))
  }

  def deleteLogically(): Unit = {
    executeDeleteLogically(None)
  }

  def revert(period: Int): Unit = {
    executeRevert(period)
  }

  def revertLastChange(period: Int): Unit = {
    executeRevert(period)
  }

  def vacuum(retentionPeriod: Int): Unit = {
    executeVacuum(Some(retentionPeriod))
  }

  def vacuumAll(): Unit = {
    executeVacuum(Some(0))
  }

  def update(set: Map[String, Column]): Unit = ???

  def update(condition: String, set: Map[String, Column]): Unit = ???

}

object Table {
  def fromPath(path: String)(implicit spark: SparkSession): Table = {
    new Table(spark.read.parquet(path), path)
  }

  def fromName(name: String)(implicit spark: SparkSession): Table = {
    spark.sql(s"msck repair table $name")
    new Table(spark.read.table(name), getTableLocation(name))
  }

  def getTableLocation(name: String)(implicit spark: SparkSession): String = {
    spark.sql(s"desc formatted $name").toDF
      .filter(col("col_name") === "Location")
      .collect()(0)(1)
      .toString
  }
}
