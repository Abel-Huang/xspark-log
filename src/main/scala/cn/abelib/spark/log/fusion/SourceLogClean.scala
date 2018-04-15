package cn.abelib.spark.log.fusion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by ${abel-huang} on 18/4/10.
  *  生成最终的DF 将日志按天进行划分 并且进行存储
  */
object SourceLogClean {
  // /Users/l2016045/Downloads/data/fusion-clean-log.log
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val originalPath = "/Users/l2016045/Downloads/data/fusion-clean-log"
    val savePath = "/Users/l2016045/Downloads/data/final-clean-log"

    val spark = SparkSession.builder().master("local").appName("LogClean").getOrCreate()
    val orginalRDD = spark.sparkContext.textFile(originalPath)
    //orginalRDD.take(10).foreach(println)

    //to DF
    val fusionDF = spark.createDataFrame(orginalRDD.map(x => LogParser.parseLog(x)), LogParser.struct)

    //    fusionDF.printSchema()
    //    // false 不会截取超过场地的字符串
        fusionDF.show(false)

    // 将清理后的日志数据 以parquet 写入到文件系统  这里写入的是本机系统, 每次写到10个文件中
    fusionDF.coalesce(10).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(savePath)

    spark.stop()

  }

}
