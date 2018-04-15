package cn.abelib.spark.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by ${abel-huang} on 18/4/15.
  */
object TopCountJob {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val srcPath = "/Users/l2016045/Downloads/data/final-clean-log"

    val spark = SparkSession.builder().master("local").appName("LogClean")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    val accessDF = spark.read.format("parquet").load(srcPath)

//    accessDF.printSchema()
//    accessDF.show(false)

    topDailyTrafficDF(spark, accessDF)

    topDailyTrafficSQL(spark, accessDF)
    spark.stop()
  }

  /**
    *   访问最多的资源
    *   使用SQL完成统计
    * @param spark
    * @param df
    */
  def topDailyVisitBySQL(spark: SparkSession, df: DataFrame):Unit = {
    df.createOrReplaceTempView("access_logs")
    val topVisit =  spark.sql("select day, url, count(1) as times from access_logs " +
      "where day= '20180407' " +
      "group by day, url order by times desc")
    topVisit.show(false)
  }

  /**
    *   使用DF完成统计
    * @param spark
    * @param df
    */
  def topDailyVisitByDF(spark: SparkSession, df: DataFrame):Unit = {
    import spark.implicits._
    val topVisit = df.filter($"day" === "20180407")
      .groupBy("day", "url")
      .agg(count("url").as("times"))
      .orderBy($"times".desc)
    topVisit.show(false)
  }

  /**
    *   按照流量进行统计
    *   使用DataFrame API
    * @param spark
    * @param df
    */
  def topDailyTrafficDF(spark: SparkSession, df: DataFrame): Unit ={
    import spark.implicits._
    val topTraffic = df.filter($"day" === "20180407")
      .groupBy("day", "url")
      .agg(sum("size").as("traffics"))
      .orderBy($"traffics".desc)
    topTraffic.show(false)
  }

  /**
    * 使用SparkSQL
    * @param spark
    * @param df
    */
  def topDailyTrafficSQL(spark: SparkSession, df: DataFrame): Unit ={
    df.createOrReplaceTempView("access_logs")
    val topTraffic =  spark.sql("select day, url, sum(size) as traffics from access_logs " +
      "where day= '20180407' " +
      "group by day, url order by traffics desc")
    topTraffic.show(false)
  }
}
