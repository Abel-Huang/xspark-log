package cn.abelib.spark.log.fusion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/10.
  * 原始日志格式解析
  */
object SourceLogFormat {
  ///  存储在本地文件系统上   /Users/l2016045/Downloads/data
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val originalPath = "/Users/l2016045/Downloads/data/fusion-log.log"
    val savePath = "/Users/l2016045/Downloads/data/fusion-clean-log"

    val spark = SparkSession.builder().appName("LogFormat").master("local").getOrCreate()

    val access = spark.sparkContext.textFile(originalPath)
    // 用前10个数据测试一下访问
    //access.take(10).foreach(println)

    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val hit = splits(1)
      val responseTime = splits(2)
      val time = splits(3) + " " + splits(4)
      val method = splits(5).replaceAll("\"", "");
      val url = splits(6)
      val protocol = splits(7).replaceAll("\"", "");
      val status = splits(8)
      val size = splits(9)
      val referer = splits(10).replaceAll("\"", "");
      val ua = splits.slice(11, splits.length).mkString(" ").replaceAll(",", " ")

      //(ip, hit, responseTime, DateUtil.parse(time), method, url, protocol, status, size, referer, ua)
      ip + "," + hit+ "," + responseTime+ "," + DateUtil.parse(time) + "," + method + "," +  url + "," + protocol + "," + status+ "," + size+ "," + referer+ "," + ua + ")"
    }).saveAsTextFile(savePath)

    spark.stop()
  }

}
