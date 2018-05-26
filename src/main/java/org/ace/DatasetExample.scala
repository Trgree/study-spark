package org.ace

import org.apache.spark.sql.SparkSession

object DatasetExample {
  def main(args: Array[String]): Unit = {
    println("start ...")
    val spark = SparkSession.builder().appName("DatasetExample").master("local").getOrCreate()
    val people = spark.read.option("header", true).csv("data/people.txt")
    val department = spark.read.option("header", true).csv("data/department.txt");
    people.show()
    department.show()

    import org.apache.spark.sql.functions._
    people.filter(people("age")>20)
      .join(department, people("deptId") === department("id"))
      .groupBy(department("name"),people("sex"))
      .agg(avg(people("age")),max(people("age"))).show()

    spark.stop()
    println("finish")
  }
}
