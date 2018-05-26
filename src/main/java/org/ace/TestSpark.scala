package org.ace

import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mySpark")
    conf.setMaster("local")
    val sc =new SparkContext(conf)
    val rdd =sc.parallelize(List(1,2,3,4,5,6)).map(_*3)
    //对集合求和
    println(rdd.reduce(_+_))
    println("math is work")
  }
}
