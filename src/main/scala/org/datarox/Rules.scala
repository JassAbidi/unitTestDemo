package org.datarox

import org.apache.spark.sql.DataFrame

object Rules {

  def getEmployeeBySalary(employeesData: DataFrame, maxSalary: Long): DataFrame = {
    employeesData.filter(employeesData.col("salary") <= maxSalary)
  }
}
