package cn.abelib.spark.exsource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/9.
  */
object SchemaInfoApp {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate();
    val result = spark.read.format("json").load("/Users/l2016045/Downloads/data/test.json")
    println(spark)
    // 输出对应的Schema信息
    result.printSchema()

    result.show()
    spark.stop()
  }

  //通过反射获取类的信息
  case class People(id: Int, name: String, age: Int)
}
