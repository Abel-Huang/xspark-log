package cn.abelib.unuse

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wf on 2017/1/12.
  */
object yitianyike {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    val sqlContext = SQLContext.getOrCreate(sc)
    val rdd = sc.textFile("/Users/wf/Downloads/yitianyike.txt")
    val logrdd = rdd.flatMap(x => {
      val ret = x.split(" ")
      val ua = ret(11).substring(1, ret(11).length - 1)
      if (ua.indexOf("+") < 0) {
        Seq()
      } else {
        val sdk = ua.substring(0, ua.indexOf("+"))
        if (ua.indexOf("(") < 0 || ua.indexOf(")") < 0) {
          Seq()
        } else {
          val sysinfo = ua.substring(if (ua.indexOf("(") < 0) 0 else ua.indexOf("(") + 1, if (ua.indexOf(")") < 0) 0 else ua.indexOf(")")).split(";")
          if (sysinfo.length < 4) {
            println(ua)
            Seq()
          } else {
            Seq(Log(ret(0), ret(2).toInt, ret(3).substring(1), ret(5).substring(1), ret(6), ret(7).substring(0, ret(7).length - 1), ret(8).toInt, ret(9).toLong, ret(10).substring(1, ret(10).length - 1), sdk, sysinfo(0), sysinfo(1), sysinfo(2), sysinfo(3)))
          }
        }
      }
    })

    val df = sqlContext.createDataFrame(logrdd)
    df.registerTempTable("fusion")
    df.sqlContext.sql("select * from fusion").collect().foreach(println)
  }

  case class Log(ip: String, resptime: Int, dt: String, method: String, url: String, scheme: String, code: Int, size: Long, refer: String, sdk: String, os: String, unkone: String, system: String, machine: String)
}
