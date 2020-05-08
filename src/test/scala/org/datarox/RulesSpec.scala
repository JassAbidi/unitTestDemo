package org.datarox

import org.apache.spark.sql.SparkSession
import org.datarox.Rules._
import org.scalatest._

case class Employee(name: String, position: String, salary: Long)

class RulesSpec extends FlatSpec with Matchers with GivenWhenThen {

  val spark = SparkSession
    .builder()
    .appName("rules-test")
    .master("local[*]")
    .getOrCreate()

  val employees = Seq(
    Employee("Dhekra", "CEO", 15000),
    Employee("Jasser", "Data Engineer", 4000),
    Employee("Nidhal", "intern", 1200),
    Employee("Ahmed", "Data Analyst", 3000)
  )

  val expectedResult = Seq(
    Employee("Ahmed", "Data Analyst", 3000),
    Employee("Nidhal", "intern", 1200)
  )


  "getEmployeeBySalary" should "filter all employees with salary lower then a given salary" in {
    import spark.implicits._
    Given("employees data, max salary")
    val employeesData = employees.toDF()
    val maxSalary = 3000
    When("getEmployeeBySalary is invoked")
    val employeesWithSalaryLowerThan3000 = getEmployeeBySalary(employeesData, maxSalary)
    Then("all employees with salary lower then 3000 should be returned")
    val expectedResultDf = expectedResult.toDF()
    employeesWithSalaryLowerThan3000.collect() should contain theSameElementsAs expectedResultDf.collect()
  }
}