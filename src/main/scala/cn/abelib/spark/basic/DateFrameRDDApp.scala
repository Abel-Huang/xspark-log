package cn.abelib.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/7.
  * DataFrame和RDD的互相转换
  */
object DateFrameRDDApp {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate();
    val result = spark.read.textFile("/Users/l2016045/Downloads/result.txt")
    // 输出对应的Schema信息
    result.printSchema()
    // 输出前20条数据
    result.show()

    import spark.implicits._ //导入隐式转换
    val people = result.map(_.split(",")).map(line => People(line(0).toInt, line(1), line(2).toInt)).toDF()
    people.printSchema()
    people.show()

    people.filter(people.col("age")<23).show()
    spark.stop()
  }

  //通过反射获取类的信息
  case class People(id: Int, name: String, age: Int)
}
