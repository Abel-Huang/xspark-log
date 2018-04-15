package cn.abelib.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wf on 2017/1/15.
  */
object Learn01 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  //def max = (x:Int, y: Int) => Int = if (x > y) x else y
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local"))
    val inputRDD = sc.textFile("/Users/l2016045/Downloads/test.txt")
    val javaRDD = inputRDD.filter(line => line.contains("Java"))
    val springRDD = inputRDD.filter(line => line.contains("Spring"))
    println(javaRDD.count())

    val resultRDD = javaRDD.union(springRDD)
    println(resultRDD.count())

    resultRDD.take(10).foreach(println)
  }

}
