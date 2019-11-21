package org.datarox

import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.datarox.Rules._
case class User(nom: String, prenom: String, age: Int)


class RulesSpec extends FlatSpec with Matchers with GivenWhenThen {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val users = Seq(
    User("BEN JEBARA", "Ahmed", 25),
    User("ABIDI", "Jasser", 27),
    User("SADAAOUI", "Nidhal", 21),
    User("MASMOUDI", "Zaineb", 23)
  )

  val usersUnder23 = Seq(
    User("MASMOUDI", "Zaineb", 23),
    User("SADAAOUI", "Nidhal", 21)
  )

  "filterOnAge" should "filter dataframe on age" in {
    import spark.implicits._
    Given("input dataframe and age")
    val age = 23
    val inputDataframe = users.toDF

    When("filterOnAge is invoked")
    val result = filterOnAge(inputDataframe, age)

    Then("users with age under 23 should be returned")
    result.collect() should contain theSameElementsAs  usersUnder23.toDF().collect()
  }



}