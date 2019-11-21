package org.datarox

import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait SharedSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  private var _sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
     _sparkSession = SparkSession
      .builder()
      .appName("test_Session")
         .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_sparkSession != null) {
      _sparkSession.stop()
      _sparkSession = null
    }
    super.afterAll()
  }

  def sparkSession: SparkSession = _sparkSession

}