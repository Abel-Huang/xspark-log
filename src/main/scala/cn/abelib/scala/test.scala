package cn.abelib.scala


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ${abel-huang} on 18/4/2.
  */
object test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    println("Hello world")
    val sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local"))
    val lines = sc.parallelize(List("pandas", "I hate pyhton"))
    println(lines.name)


  }

}
