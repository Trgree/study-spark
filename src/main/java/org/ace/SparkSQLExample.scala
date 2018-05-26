package org.ace

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object SparkSQLExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkSQLExample").getOrCreate()


    runBasicDataFrameExample(spark)
    runDatasetCreationExample(spark)
    runProgrammaticSchemaExample(spark)
    runInferSchemaExample(spark)
    spark.stop()

  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val people = spark.read.option("header", "true").csv("data/people.txt")
    people.show()
    people.printSchema()

    people.select("name").show()

    import spark.implicits._    // This import is needed to use the $-notation
    people.select($"name", $"age" + 1).show() //
    people.filter($"age" > 20).show()
    people.filter(people("age") > 20).show() // 以上一句一样效果
    people.groupBy("sex").count().show()

    // 创建表，并使用sql
    people.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people") // lifetime和sparkSession一致
    sqlDF.show()

    // 创建global的表
    people.createGlobalTempView("people") // lifetime和Application一致,需要使用global_temp.
    spark.sql("select * from global_temp.people").show()
    spark.newSession().sql("select * from global_temp.people").show()

  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val caseClassDS = Seq(Person("Andy",23)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.show()
    println(primitiveDS.map(_+1).collect())

    val people = spark.read.json("data/people.json").as[Person] // 自定义类的Dataset
    people.show()
    people.filter(_.age > 20 ).show() // 可以使用Person的属性
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    val peopleRDD = spark.sparkContext.textFile("data/people2.txt")
    peopleRDD.collect().foreach(println)

    // 创建schema
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(field => StructField(field, StringType, nullable = true))
    val schema = StructType(fields)

    //RDD转换为DataFrame
    val rowRDD = peopleRDD.map(_.split(",")).map(arr => Row(arr(0),arr(1).trim))
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.show()

    peopleDF.createOrReplaceTempView("people")
    val result = spark.sql("select * from people")
    import spark.implicits._

    // DataFrame[Row] -> DataFrame[String]
    result.map(att => "name:" + att(0)).show() // 需要 import spark.implicits._，否则不能自动转为String

  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // RDD[Person] -> DataFrame
    val peopleRDD = spark.sparkContext.textFile("data/people2.txt")
      .map(_.split(",")).map(arr => Person(arr(0), arr(1).toInt))

    val peopleDF = peopleRDD.toDF() // DataFrame
    peopleDF.show()

    peopleDF.createOrReplaceTempView("people")
    spark.sql("select * from people")
    peopleDF.map(p => "name:" + p.get(0)).show()
    peopleDF.map(p => "name:" + p.getAs[String]("name")).show()

    val peopleDS = peopleRDD.toDS() // Dataset
    peopleDS.map(p => "name" + p.name).show()  // Dataset中，可以使用 类型的属性
    peopleDS.map(p => Person(p.name, p.age +1)).show()

    //   No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]] // 定义 Endocer
    peopleDF.map(p => p.getValuesMap[Any](List("name", "age"))).collect().foreach(println)  // 每一行转换为Map
  }

}

