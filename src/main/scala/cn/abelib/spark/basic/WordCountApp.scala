package cn.abelib.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ${abel-huang} on 18/4/7.
  *   基于Spark的WordCount
  */
object WordCountApp {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local"))
    val spark = SparkSession
      .builder()
      .getOrCreate();
    val file = spark.sparkContext.textFile("/Users/l2016045/Downloads/test.txt")
    val wordCount = file.flatMap(line=>line.split(",")).map((word=>(word, 1))).reduceByKey(_ + _)

    wordCount.foreach{word=>println(word)}
  }

  SQLContext
}
