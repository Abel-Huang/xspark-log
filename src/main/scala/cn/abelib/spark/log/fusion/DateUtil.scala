package cn.abelib.spark.log.fusion

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
/**
  * Created by ${abel-huang} on 18/4/10.
  * 针对日期进行解析
  */
object DateUtil {
  // 原始日志格式
  // 07/Apr/2018:21:47:04 +08002691
  // FastDateFormat 线程安全
  val TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  // 目标日志格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }


  def getTime(time:String) = {
    try {
      TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception =>{
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("[07/Apr/2018:22:38:09 +0800]"))
  }
}
