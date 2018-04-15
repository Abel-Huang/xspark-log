//import java.net.URI
//
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.fs.qiniu.QiniuFileSystem
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//import org.joda.time._
//
///**
//  * Created by wf on 2017/1/8.
//  */
//object Hello {
//  // --------------------------------------------------------------------------------------------------
//  val sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local[4]"))
//  val sqlContext = SQLContext.getOrCreate(sc)
//
//  ///  以上变量, xspark 环境中已存在,不用在新建 ///
//  def getKey = (System.getenv("yitianyikeAk"), System.getenv("yitainyikeSk"))
//
//  val ak = getKey._1
//  val sk = getKey._2
//  //  val ipfile = "/usr/spark/3rd/17monipdb.dat"
//  val ipfile = "17monipdb.dat"
//  // 以上信息,除 ipfile 外, 开发环境 和 Zeppelin 代码不一样。
//  // Zeppelin 不需要 object YiTianYiKeAnalyser {} 块
//  // Zeppelin 不需要 main 方法块,需将方法体的内容拿出来
//  // --------------------------------------------------------------------------------------------------
//  val bucket = "qiniu://fusionlog"
//  val path = "http://fusionlog.qiniu.com"
//  val devices = s"${bucket}/yitianyike-devices-channel.csv"
//  sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
//  sc.hadoopConfiguration.set("fs.qiniu.access.key", ak)
//  sc.hadoopConfiguration.set("fs.qiniu.secret.key", sk)
//  sc.hadoopConfiguration.set("fs.qiniu.bucket.domain", path)
//  sc.hadoopConfiguration.set("fs.qiniu.file.retention", "55")
//  val fs = new QiniuFileSystem();
//  fs.initialize(new URI(s"$bucket/"), sc.hadoopConfiguration)
//
//  def deleteFile(delUrl: String, recursive: Boolean = true) = {
//    println(s"开始删除文件 $delUrl: ${new DateTime()}")
//    fs.delete(new Path(delUrl), recursive)
//    println(s"删除文件 $delUrl 完成: ${new DateTime()}")
//  }
//
//  // =============================================================
//  // 分表
//  val channelRegionUsercountType = "ChannelRegionUsercount"
//  val channelProvinceUsercountType = "ChannelProvinceUsercount"
//  val modelUsercountType = "ModelUsercount"
//  val channelSystemUsercountType = "ChannelSystemUsercount"
//  val channelDownloadType = "ChannelDownload"
//  val channelTopKeyDownloadType = "ChannelTopKeyDownload"
//  val channelTopKeyFluxType = "ChannelTopKeyFlux"
//  val channelRestimeType = "ChannelRestime"
//  val channelStatusType = "ChannelStatus"
//
//  def hourParquetSuffix(subject: String) = s"$bucket/parquet/$subject/hour"
//
//  // 注册主表，包含所有数据
//  def registerTable(parquetUrl: String, tableName: String) = {
//    println(s" 开始注册 $parquetUrl 到表 $tableName: ${new DateTime()}")
//    sqlContext.read.parquet(parquetUrl).registerTempTable(tableName)
//    println(s" 注册 $parquetUrl 到表 $tableName 完成: ${new DateTime()}")
//  }
//
//  case class ChannelRegionUsercount(channel: String, model: String, nation: String,
//                                    province: String, city: String, userCount: Long, time: Int)
//
//  def createChannelRegionUsercount(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val model = r.getString(1)
//      val nation = r.getString(2)
//      val province = r.getString(3)
//      val city = r.getString(4)
//      val userCount = r.getLong(5)
//      val time = r.getInt(6)
//      ChannelRegionUsercount(channel, model, nation, province, city, userCount, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelProvinceUsercount(channel: String, province: String, userCount: Long, time: Int)
//
//  def createChannelProvinceUsercount(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val province = r.getString(1)
//      val userCount = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelProvinceUsercount(channel, province, userCount, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ModelUsercount(channel: String, model: String, userCount: Long, time: Int)
//
//  def createModelUsercount(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val model = r.getString(1)
//      val userCount = r.getLong(2)
//      val time = r.getInt(3)
//      ModelUsercount(channel, model, userCount, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelSystemUsercount(channel: String, system: String, userCount: Long, time: Int)
//
//  def createChannelSystemUsercount(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val system = r.getString(1)
//      val userCount = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelSystemUsercount(channel, system, userCount, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelDownload(channel: String, count: Long, time: Int)
//
//  // 分渠道的总下载次数
//  def createChannelDownload(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val count = r.getLong(1)
//      val time = r.getInt(2)
//      ChannelDownload(channel, count, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelTopKeyDownload(channel: String, key: String, count: Long, time: Int)
//
//  // 分渠道的 top xxx 的下载次数
//  def createChannelTopKeyDownload(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val key = r.getString(1)
//      val count = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelTopKeyDownload(channel, key, count, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelTopKeyFlux(channel: String, key: String, flux: Long, time: Int)
//
//  // 分渠道的 top xxx 的流量
//  def createChannelTopKeyFlux(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val key = r.getString(1)
//      val flux = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelTopKeyFlux(channel, key, flux, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelRestime(channel: String, size: Long, restime: Long, time: Int)
//
//  // 下载的总大小、总耗时
//  def createChannelRestime(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val size = r.getLong(1)
//      val restime = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelRestime(channel, size, restime, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  case class ChannelStatus(channel: String, code: Int, count: Long, time: Int)
//
//  // 渠道、状态码、状态码个数
//  def createChannelStatus(sql: String, writeUrl: String) = {
//    println(sql, writeUrl)
//    deleteFile(writeUrl)
//    println(s"开始汇总表: $writeUrl : ${new DateTime()}")
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val code = r.getInt(1)
//      val count = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelStatus(channel, code, count, time)
//    })
//    sqlContext.createDataFrame(reginrdd).write.parquet(writeUrl)
//    println(s"汇总表: $writeUrl 完成 : ${new DateTime()}")
//  }
//
//  def createHourTable(domain: String, datetime: DateTime): Unit = {
//    val timeSuffix = datetime.toString("yyyy/MM/dd/HH")
//    val time = datetime.getMillis / 1000 / 3600 * 3600
//    val parquetOriginUrl = s"$bucket/parquet/${domain}/${timeSuffix}/*"
//    val writeParquetUrlSuffix = hourParquetSuffix(domain)
//    val tableName = "ytyk_temp_hour"
//    println(s"开始汇总小时表: $parquetOriginUrl to $writeParquetUrlSuffix : $tableName  : ${new DateTime()}\n")
//    registerTable(parquetOriginUrl, tableName)
//    val sqlChannelRegionUsercount = s" select channel, model, nation, province, city, count(DISTINCT user) as userCount, " +
//      s" ${time} as time from ${tableName} where nation = '中国' group by  channel, model, nation, province, city "
//    createChannelRegionUsercount(sqlChannelRegionUsercount, s"$writeParquetUrlSuffix/$channelRegionUsercountType/$timeSuffix")
//    val sqlChannelProvinceUsercount = s" select channel, province, count(DISTINCT user) as userCount, ${time} as time " +
//      s" from ${tableName} where nation = '中国' group by  channel, province "
//    createChannelProvinceUsercount(sqlChannelProvinceUsercount, s"$writeParquetUrlSuffix/$channelProvinceUsercountType/$timeSuffix")
//    val sqlModelUsercount = s" select channel, model, count(DISTINCT user) as userCount, ${time} as time " +
//      s" from ${tableName} where nation = '中国' group by  channel, model "
//    createModelUsercount(sqlModelUsercount, s"$writeParquetUrlSuffix/$modelUsercountType/$timeSuffix")
//    val sqlChannelSystemUsercount = s" select channel, system, count(DISTINCT user) as userCount, ${time} as time " +
//      s" from ${tableName} where nation = '中国' group by  channel, system "
//    createChannelSystemUsercount(sqlChannelSystemUsercount, s"$writeParquetUrlSuffix/$channelSystemUsercountType/$timeSuffix")
//    // 带宽： 5 分钟流量 / 300 * 8 bps
//    // 小时带宽， 取小时内 5 分钟带宽的最大值
//    // 暂不做
//    // ChannelBandwith
//    // channel, bandwith, time
//    // 分渠道的总下载次数
//    val sqlChannelDownload = s" select channel, count(1) as count, ${time} as time " +
//      s" from ${tableName} where nation = '中国' group by  channel "
//    createChannelDownload(sqlChannelDownload, s"$writeParquetUrlSuffix/$channelDownloadType/$timeSuffix")
//    //    // 分渠道的 top 500 的下载次数
//    //    select  channel, key, count from (
//    //      select channel, key, count,  row_number() over(partition by channel order by count desc) idx  from (
//    //      select channel, key, count(1) as count
//    //        from ytyk where nation = '中国'  group by  channel, key
//    //    ) a ) b where idx < 5 order by channel, count desc
//    val sqlChannelTopKeyDownload = s"select  channel, key, count, ${time} as time from ( " +
//      " select channel, key, count, row_number() over(partition by channel order by count desc) idx from ( " +
//      s" select channel, key, count(1) as count from ${tableName} where nation = '中国'  group by  channel, key" +
//      " ) a ) b where idx < 500 order by channel, count desc"
//    createChannelTopKeyDownload(sqlChannelTopKeyDownload, s"$writeParquetUrlSuffix/$channelTopKeyDownloadType/$timeSuffix")
//    // 分渠道的 top 500 的流量
//    val sqlChannelTopKeyFlux = s"select  channel, key, flux, ${time} as time from ( " +
//      " select channel, key, flux, row_number() over(partition by channel order by flux desc) idx from ( " +
//      s" select channel, key, sum(size) as flux from ${tableName} where nation = '中国'  group by  channel, key" +
//      " ) a ) b where idx < 500 order by channel, flux desc"
//    createChannelTopKeyFlux(sqlChannelTopKeyFlux, s"$writeParquetUrlSuffix/$channelTopKeyFluxType/$timeSuffix")
//    // 下载的总大小、总耗时
//    val sqlChannelRestime = s" select  channel, sum(size) as totalsize, sum(restime) as totalrestime, ${time} as time " +
//      s" from ${tableName} where nation = '中国' group by  channel "
//    createChannelRestime(sqlChannelRestime, s"$writeParquetUrlSuffix/$channelRestimeType/$timeSuffix")
//    // 渠道、状态码、状态码个数
//    val sqlChannelStatus = s" select channel, code, count(1), ${time} as time " +
//      s" from ${tableName} where nation = '中国' group by  channel, code "
//    createChannelStatus(sqlChannelStatus, s"$writeParquetUrlSuffix/$channelStatusType/$timeSuffix")
//    println(s"汇总小时表: $parquetOriginUrl to $writeParquetUrlSuffix : $tableName : 完成 : ${new DateTime()} \n\n")
//  }
//
//  def createDayTable(domain: String, datetime: DateTime): Unit = {
//    // val timeSuffix = datetime.toString("yyyy/MM/dd")
//    // val time = datetime.getMillis/1000/86400 * 86400
//    // val parquetOriginUrl = s"$bucket/parquet/${domain}/${timeSuffix}/*"
//    // val writeUrlSuffix = s"$bucket/parquet/$domain/day/$timeSuffix"
//    // val tableName = "ytyk_temp_day"
//    // createTable(parquetOriginUrl, tableName, time, writeUrlSuffix)
//    println("TODO 应该从小时数据统计出数据,当前是直接从原始数据统计")
//  }
//
//  // TODO 应该从天数据统计出月数据,当前是直接从小时数据统计
//  def createMonthTable(domain: String, datetime: DateTime): Unit = {
//    //    val timeSuffix = datetime.toString("yyyy/MM")
//    //    val time = new DateTime(datetime.getYear, datetime.getMonthOfYear, 0).getMillis/1000
//    //    val parquetOriginUrl = s"$bucket/parquet/${domain}/${timeSuffix}/*"
//    //    val writeUrlSuffix = s"$bucket/parquet/$domain/month/$timeSuffix"
//    //    val tableName = "ytyk_temp_month"
//    //
//    //    createTable(parquetOriginUrl, tableName, time, writeUrlSuffix)
//    println("TODO 应该从天数据统计出月数据,当前是直接从原始数据统计")
//  }
//
//  // =============================================================
//  def main(args: Array[String]): Unit = {
//    //    val data = "/Users/wf/Test/yitianyike/devices.txt"
//    //    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
//    //    val sc = new SparkContext(conf)
//    //    val kkk = sc.textFile(data).map(x => {val s = x.split(","); (s(0), s(1))})
//    //    println(kkk)
//    //    val bk = kkk.lookup("oppo").mkString
//    //    if (bk == "OPPO") {
//    //      println("--------------->>>")
//    //
//    //        val log: String = "-Mozilla/5.0+(Linux;+U;+Android+6.0;+zh-cn;GIONEE-GN8002/GIONEE-GN8002+Build/IMM76D)+AppleWebKit534.30(KHTML,like+Gecko)Version/4.0+Mobile+Safari/534.30+Id/AAC3010D557B0FC202D98648E58A517C+RV/5.0.16"
//    //    val log: String = "-Mzilla/5.0+(LinuGIOAAAE+Android+6.0;+zh-cn;GIONEE-GN8002/GIONEE-GN8002+Build/IMM76D)+AppleWebKit534.30(KHTML,like+Gecko)Version/4.0+Mobile+Safari/534.30+Id/AAC3010D557B0FC202D98648E58A517C+RV/5.0.16"
//    //        val pattern = new Regex(".*Mozilla.*|.*GIOAAAE.*")
//    //        println((pattern findAllIn log).mkString)
//    //
//    //    val log: String = "-Mozilla/5.0+(Linux;+U;+Android+6.0;+zh-cn;GIONEE-GN8002/GIONEE-GN8002+Build/IMM76D)+AppleWebKit534.30(KHTML,like+Gecko)Version/4.0+Mobile+Safari/534.30+Id/AAC3010D557B0FC202D98648E58A517C+RV/5.0.16"
//    //    val pattern = new Regex("Linux.*U[^)]*")
//    //    println((pattern findAllIn log).mkString)
//    //    def stripChars(s:String, ch:String)= s filterNot (ch contains _)
//    //    val s = "abc+cd-dfa zzz"
//    //   println(stripChars(s, "+- "))
//    //    "abc+d-e     f;en".trim().replaceAll("\\s{2,}", " ").split(Array('+', ' ', '-', ';')).foreach(println)
//    //> res1: Boolean = true
//    if (
//      """\d""".r.unapplySeq("5").isDefined) {
//      println("yes-5")
//    }
//    //> res2: Boolean = false
//    if (
//      """\d""".r.unapplySeq("a").isDefined) {
//      println("yes-a")
//    }
//    if (!"""(Android|Xhrome)[0-9.]*""".r.unapplySeq("hrome1.3.8").isDefined) {
//      println("system--111")
//    }
//    if (
//      """(Android|Xhrome)[0-9.]*""".r.unapplySeq("Xhrome1.3.8").isDefined) {
//      println("system--22")
//    }
//    //    println(" ++abc+d-e     f;en".replaceAll("[+-]", " ").replaceAll("\\s{2,}", " ").trim)
//    //    val times = 2.to(12).reverse
//    //    val now = new DateTime()
//    //    times.foreach(i => {
//    //      println(i)
//    //      val dt = now.plusHours(-i)
//    //      println(dt.toString("yyyy-MM-dd-HH"))
//    //      println(dt.toString("H"))
//    //      println(dt.toString("HH"))
//    //    })
//    //    var now = new DateTime()
//    //    now = now.plusDays(-2)
//    //    println(now)
//  }
//}