package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object DataFrameUtils {

  def createDataFrame(spark: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }
}
