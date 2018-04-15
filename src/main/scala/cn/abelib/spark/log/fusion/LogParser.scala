package cn.abelib.spark.log.fusion

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * Created by ${abel-huang} on 18/4/10.
  */
object LogParser {
  val struct = StructType(
    Array(
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("hit", StringType),
      StructField("responseTime", LongType),
      StructField("time", StringType),
      StructField("day", StringType),
      StructField("method", StringType),
      StructField("url", StringType),
      StructField("protocol", StringType),
      StructField("status", StringType),
      StructField("size", LongType),
      StructField("referer", StringType),
      StructField("ua", StringType)
    )
  )

  //解析日志， 将城市信息加入
  def parseLog(log:String) ={
    try {
      val splits = log.split(",")

      val ip = splits(0)
      val city = "-"
      val hit = splits(1)
      val responseTime = splits(2).toLong
      val time = splits(3)
      val day = time.substring(0, 10).replaceAll("-", "")
      val method = splits(4)
      val url = splits(5)
      val protocol = splits(6)
      val status = splits(7)
      val size = splits(8).toLong
      val referer = splits(9)
      val ua = splits(10)

      Row(ip, city, hit, responseTime, time, day, method, url, protocol, status, size, referer, ua)
    } catch {
      case e:Exception => Row(0)
    }
  }
}
