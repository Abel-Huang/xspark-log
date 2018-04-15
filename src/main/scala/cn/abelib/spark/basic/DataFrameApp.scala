package cn.abelib.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/7.
  *  DataFrame
  */
object DataFrameApp {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate();
    val result = spark.read.json("/Users/l2016045/Downloads/result.json")
    // 输出对应的Schema信息
    result.printSchema()
    // 输出前20条数据
    result.show()

    result.first()

    result.head(1)

    //查询某列所有的数据
    result.select("name").show()

    // 查询几列 并且进行计算
    result.select(result.col("name"), result.col("age"), (result.col("age")-10).as("num")).show()

    // 根据某一列的值进行过滤
    result.filter(result.col("age")>22).show()

    // 根据某一列进行分组 然后再进行聚合
    result.groupBy("age").count().show()

    result.filter("age=23").show()

    result.sort(result("age")).show()

    result.join(result, result.col("age") === result.col("age")).show()
    spark.stop()
  }
}
