package org.apache.spark.sql.execution.datasources

import java.util.TimeZone

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.PartitioningUtils._
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

object PartitionUtils {

  def inferPartition(spark: SparkSession, paths: Seq[Path], basePaths: Set[Path], schema: Option[StructType]) = {
    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone
    parsePartitions(
      paths,
      spark.sessionState.conf.partitionColumnTypeInferenceEnabled,
      basePaths,
      schema,
      false,
      timeZoneId)
  }


  def inferPartition(spark: SparkSession, path: Path, basePaths: Set[Path], userSpecifiedDataTypes: Map[String, DataType]) = {
    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone

    parsePartition(
      path,
      spark.sessionState.conf.partitionColumnTypeInferenceEnabled,
      basePaths,
      userSpecifiedDataTypes,
      DateTimeUtils.getTimeZone(timeZoneId))
  }

  def extractPartitions(basePath: String, filePath: String) = {
    val p = filePath.replaceAll(basePath, "")
    parsePathFragment(p)
  }
}
