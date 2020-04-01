package tables

import java.util.Calendar

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.{Column, DataFrame, DataFrameUtils, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, EqualNullSafe, Expression, InputFileName, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.DeltaLog.log
import org.apache.spark.sql.execution.datasources.PartitionUtils.inferPartition
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import TableUtils._

trait OptimizedTableOperations {
  self: OptimizedTable =>

  protected def executeDelete(condition: Option[Expression]): Unit = {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    condition match {
      case None =>
        val fileIndex = new InMemoryFileIndex(spark, Seq(new Path(self.location)), Map.empty, None)
        val allFiles = fileIndex.inputFiles
        allFiles.foreach(file => remove(file))
      case Some(cond) =>
        val fileIndex = new InMemoryFileIndex(spark, Seq(getLocation), Map.empty, None)
        val partitionColumns = fileIndex.partitionSpec().partitionColumns.map(_.name)
        val (metadataPredicates, dataPredicates) = splitMetadataAndDataPredicates(cond, partitionColumns)
        if (dataPredicates.isEmpty) {
          val touchedFiles = filterByPartitionsPredicates(fileIndex, metadataPredicates)
          touchedFiles.foreach(file => remove(file))
        }
        else {
          //  optimized algorithm :
          //  (1) get candidate files
          //  (2) construct index with candidates files
          //  (3) read data based on index and get files based on delete predicate
          //  (4) construct index of files based on file of step (3)
          //  (5) read data based on index of step (4) and filter on ! delete predicates
          //  (6) write data of step (5)


          // (1) get candidate files using filtered on condition loaded data
          // (2) load data of candidate files and apply ! condition
          // (3) remove candidate files
          // (4) write data of step (2)

          val touchedFiles = spark.read
            .parquet(self.basePath.toString)
            .filter(new Column(cond))
            .select(new Column(InputFileName()))
            .distinct().as[String].collect().toSeq

          if (touchedFiles.nonEmpty) {
            val targetData = spark.read
              .option("basePath", self.basePath.toString)
              .parquet(touchedFiles: _*)
              .filter(new Column(Not(EqualNullSafe(cond, Literal(true, BooleanType)))))

            targetData.write.mode("append").partitionBy(partitionColumns: _*).parquet(self.basePath.toString)
            touchedFiles.foreach(file => remove(file))
          }
        }
    }
  }

  protected def executeDeleteLogically(condition: Option[Expression]): Unit = {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    // (1) get candidate files using filtered on condition loaded data
    // (2) load data of candidate files and apply ! condition
    // (3) remove candidate files
    // (4) write data of step (2)
    // (5) write data

    val fileIndex = new InMemoryFileIndex(spark, Seq(getLocation), Map.empty, None)
    val partitionColumns = fileIndex.partitionSpec().partitionColumns.map(_.name)
    val cond = condition.getOrElse(Literal(true))

    val touchedFiles = spark.read
      .parquet(self.basePath.toString)
      .filter(new Column(cond))
      .select(new Column(InputFileName()))
      .distinct().as[String].collect().toSeq

    val targetData = spark.read
      .option("basePath", self.basePath.toString)
      .parquet(touchedFiles: _*)
      .filter(new Column(Not(EqualNullSafe(cond, Literal(true, BooleanType)))))

    val deletedData = spark.read
      .option("basePath", self.basePath.toString)
      .parquet(touchedFiles: _*)
      .filter(new Column(cond))
      .withColumn("timestamp", unix_timestamp())

    targetData.write.mode("append")
      .partitionBy(partitionColumns: _*)
      .parquet(self.basePath.toString)

    deletedData.write.mode("append")
      .partitionBy("timestamp" :: partitionColumns.toList: _*)
      .parquet(self.archivePath.toString)

    touchedFiles.foreach(file => remove(file))
  }

  protected def executeRevert(period: Int): Unit = {
    // (1) get list of files to revert
    // (2) build destination path from given file path ( remove archive et timestamp from path )

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val now = Calendar.getInstance()
    now.add(Calendar.HOUR, -period)
    val threshold = now.getTimeInMillis / 1000

    val fileIndex = new InMemoryFileIndex(spark, Seq(self.archivePath), Map.empty, None)

    if (!fileIndex.inputFiles.isEmpty) {
      val touchedFiles = spark.read
        .parquet(self.archivePath.toString)
        .filter('timestamp >= threshold)
        .select(new Column(InputFileName()))
        .distinct().as[String].collect()

      if (!touchedFiles.isEmpty) {
        touchedFiles.map(path => move(path, constructOriginalPath(path)))
      }
    }
  }

  protected def executeRevertLastChange(period: Int): Unit = ???

  protected def executeVacuum(retentionPeriod: Option[Int]): Unit = {
    // (1) get list of files to revert
    // (2) build destination path from given file path ( remove archive et timestamp from path )

    // default retention period is 6 years
    val defaultRetentionPeriod = 52560
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val now = Calendar.getInstance()
    now.add(Calendar.HOUR, -retentionPeriod.getOrElse(defaultRetentionPeriod))
    val threshold = now.getTimeInMillis / 1000

    val fileIndex = new InMemoryFileIndex(spark, Seq(self.archivePath), Map.empty, None)

    if (!fileIndex.inputFiles.isEmpty) {
      val touchedFiles = spark.read
        .parquet(self.archivePath.toString)
        .filter('timestamp <= threshold)
        .select(new Column(InputFileName()))
        .distinct().as[String].collect()

      if (!touchedFiles.isEmpty) {
        touchedFiles.map(path => remove(path))
      }
    }
  }


  def constructOriginalPath(archivePath: String): String = {
    val path = new Path(archivePath)
    val fileName = path.getName

    archivePath.split("/")
      .filterNot(element => element == "archive" || element.contains("timestamp"))
      .mkString("/")
  }

  protected def sparkSession: SparkSession = self.toDF().sparkSession

  protected def getLocation = self.basePath
}
