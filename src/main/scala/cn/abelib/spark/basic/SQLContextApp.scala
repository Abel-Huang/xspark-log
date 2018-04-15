package cn.abelib.spark.basic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ${abel-huang} on 18/4/7.
  *   SQLContext是Spark 1.0的入口
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    // 本地开发环境
    sparkConf.setAppName("SQLContextAPP").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val result = sqlContext.read.format("json").load("/Users/l2016045/Downloads/result.txt")
    result.printSchema()
    result.show()

    sc.stop()
  }

}
