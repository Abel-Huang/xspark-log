package cn.abelib.spark.exsource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by ${abel-huang} on 18/4/9.
  */
object ParquetApp {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val path = "/Users/l2016045/Downloads/spark/examples/src/main/resources/users.parquet"

    val spark = SparkSession.builder().master("local").getOrCreate();

    val userDF = spark.read.parquet(path)
    userDF.printSchema()
    val result = userDF.select("name", "favorite_numbers")
    result.write.format("json").mode("append").save("/Users/l2016045/Downloads/data/user.json")
    result.show()

  }
}
