package cn.abelib.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/7.
  */
object DataFrameCase {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate();
    val log = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/l2016045/Downloads/mylog.log")

    log.printSchema()
    log.show()
    println(log.count())

    log.groupBy("sensor").count().show()
    spark.stop()
  }
}
