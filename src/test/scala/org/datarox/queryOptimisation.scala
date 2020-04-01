package org.datarox

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}


case class Post(postId: Long, profileId: Long, interactions: Int, date: String)

class queryOptimisation extends FlatSpec with Matchers with GivenWhenThen with SharedSparkSession {

  val posts = Seq(
    Post(1, 1, 20, "2019-01-01"),
    Post(2, 1, 30, "2019-02-01"),
    Post(3, 1, 40, "2019-01-01"),
    Post(4, 2, 20, "2019-01-01"),
    Post(5, 2, 10, "2019-01-01"),
    Post(6, 3, 10, "2019-01-01"),
    Post(7, 3, 10, "2019-01-01"),
    Post(8, 3, 10, "2019-01-01")
  )

  "" should "combined filters " in {
    val spark = sparkSession
    import spark.implicits._

    posts
      .toDF()
      .groupBy('profileId)
      .agg(sum('interactions).as("sum"))
      .filter('sum <= 50 || 'sum > 80)
      .show()

    Thread.sleep(1000000000)
  }

  "" should "splitted filters" in {
    val spark = sparkSession
    import spark.implicits._

    spark.conf.set("spark.sql.optimizer.excludedRules",
      "org.apache.spark.sql.catalyst.optimizer.PushDownPredicate")

    posts
      .toDF()
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\posts")
/*
    postsDf
      .groupBy('profileId)
      .agg(sum('interactions).as("sum"))
      .filter('profileId < 2)
      .show()*/

    val postsDf = spark.read.parquet("C:\\Users\\EliteBook\\IdeaProjects\\unittestdemo\\target\\posts")
      .groupBy('profileId)
      .agg(sum('interactions).as("sum"))
      .cache()
  //  postsDf.first()

    val under50Df =
    postsDf
      .filter('sum <= 50)
      .filter('profileId < 2)

    val over80Df =
    postsDf
        .filter('sum > 80)

    under50Df
      .union(over80Df)
      .show(20)

   Thread.sleep(1000000000)
  }
}
