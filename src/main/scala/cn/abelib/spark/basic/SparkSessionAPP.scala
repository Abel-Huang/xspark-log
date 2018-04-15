package cn.abelib.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/7.
  *   SparkSession 是SparkSQL 2.0的入口
  */
object SparkSessionAPP {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionAPP")
        .master("local")
      .getOrCreate()

    val result = spark.read.json("/Users/l2016045/Downloads/result.json")
    result.printSchema()
    result.show()

    val names = spark.sql("select name from result")
    names.show()

    spark.stop()
  }

}
