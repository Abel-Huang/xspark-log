//import java.net.URI
//import java.security.MessageDigest
//
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.fs.qiniu.QiniuFileSystem
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext, SparkException}
//import org.joda.time.DateTime
//import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
//import qiniu.ip17mon.Locator
//
//import scala.annotation.switch
//import scala.collection.mutable.ArrayBuffer
//import scala.util.matching.Regex
//
/////** 定时任务在 6 分钟，每小时执行一次。避开整点, 如部分机器是 23:59:59 部分机器是 00:00:01 */
//
///**
//  * 依赖包在 lib 目录下,  hadoop-qiniu-2.7.2 编译时修改了依赖,直接使用代码库的导入有包冲突;
//  *
//  * 解决 org.xerial.snappy 错误。本地环境 :
//  * 菜单: Run -> Edit Configurations -> YiTianYiKeAnalyser -> VM options 设置:
//  * -Dorg.xerial.snappy.tempdir=/tmp -Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib
//  *
//  * ak,sk
//  * Run -> Edit Configurations -> YiTianYiKeAnalyser -> Environment variables
//  * 添加  yitianyikeAk  、 yitainyikeSk
//  */
//object YiTianYiKeAnalyser {
//  // --------------------------------------------------------------------------------------------------
//  val sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local[4]"))
//  val sqlContext = SQLContext.getOrCreate(sc)
//
//  ///  以上变量, xspark 环境中已存在,不用在新建 ///
//
//  def getKey = (System.getenv("yitianyikeAk"), System.getenv("yitainyikeSk"))
//
//
//  val ak = getKey._1
//  val sk = getKey._2
//
//  //  val ipfile = "/usr/spark/3rd/17monipdb.dat"
//  //  val ipfile = "17monipdb.dat"
//  val ipfile = "http://ok0n6ytwh.bkt.clouddn.com/17monipdb.dat"
//  // 以上信息,除 ipfile 外, 开发环境 和 Zeppelin 代码不一样。
//  // Zeppelin 不需要 object YiTianYiKeAnalyser {} 块
//  // Zeppelin 不需要 main 方法块,需将方法体的内容拿出来
//  // --------------------------------------------------------------------------------------------------
//
//
//  val bucket = "qiniu://fusionlog"
//  val path = "http://fusionlog.qiniu.com"
//  val devices = s"${bucket}/yitianyike-devices-channel.csv"
//
//  sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
//  sc.hadoopConfiguration.set("fs.qiniu.access.key", ak)
//  sc.hadoopConfiguration.set("fs.qiniu.secret.key", sk)
//  sc.hadoopConfiguration.set("fs.qiniu.bucket.domain", path)
//  sc.hadoopConfiguration.set("fs.qiniu.file.retention", "55")
//
//  val fs = new QiniuFileSystem();
//  fs.initialize(new URI(s"$bucket/"), sc.hadoopConfiguration)
//
//  def info(x: Any) = {
//    //    println(x)
//  }
//
//  def info1(x: Any) = {
//    println(x)
//  }
//
//  def warn(x: Any) = {
//    //    Console.err.println(x)
//    println(x)
//  }
//
//  def deleteFile(delUrl: String, recursive: Boolean = true) = {
//    info(s"开始删除文件 $delUrl* : ${new DateTime()}")
//    fs.delete(new Path(delUrl), recursive)
//    info(s"删除文件完成 $delUrl* : ${new DateTime()}")
//  }
//
//
//  // =============================================================
//  // 分表
//
//  val channelRegionUsercountType = "ChannelRegionUsercount"
//  val channelSystemUsercountType = "ChannelSystemUsercount"
//  val channelDownloadType = "ChannelDownload"
//  val channelTopKeyDownloadType = "ChannelTopKeyDownload"
//  val channelTopKeyFluxType = "ChannelTopKeyFlux"
//  val channelRestimeType = "ChannelRestime"
//  val channelStatusType = "ChannelStatus"
//
//  val types = List(channelRegionUsercountType, channelSystemUsercountType, channelDownloadType,
//    channelTopKeyDownloadType, channelTopKeyFluxType, channelRestimeType, channelStatusType)
//
//
//  // 注册主表，包含所有数据
//  def registerTable(parquetUrl: String, tableName: String) = {
//    info(s" 开始注册 $parquetUrl 到表 $tableName: ${new DateTime()}")
//    sqlContext.read.parquet(parquetUrl).registerTempTable(tableName)
//    info(s" 注册 $parquetUrl 到表完成 $tableName: ${new DateTime()}")
//  }
//
//  case class ChannelRegionUsercount(channel: String, model: String, nation: String,
//                                    province: String, city: String, userCount: Long, time: Int)
//
//  def alreadyAnalysed(writeUrl: String) = {
//    fs.exists(new Path(s"$writeUrl/_SUCCESS")) || fs.exists(new Path(s"${writeUrl}_SUCCESS"))
//  }
//
//  def createChannelRegionUsercount(sql: String, writeUrl: String) = {
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
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//
//  case class ChannelSystemUsercount(channel: String, system: String, userCount: Long, time: Int)
//
//  def createChannelSystemUsercount(sql: String, writeUrl: String) = {
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val system = r.getString(1)
//      val userCount = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelSystemUsercount(channel, system, userCount, time)
//    })
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//  case class ChannelDownload(channel: String, count: Long, time: Int)
//
//  // 分渠道的总下载次数
//  def createChannelDownload(sql: String, writeUrl: String) = {
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val count = r.getLong(1)
//      val time = r.getInt(2)
//      ChannelDownload(channel, count, time)
//    })
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//  case class ChannelTopKeyDownload(channel: String, key: String, count: Long, time: Int)
//
//  // 分渠道的 top xxx 的下载次数
//  def createChannelTopKeyDownload(sql: String, writeUrl: String) = {
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val key = r.getString(1)
//      val count = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelTopKeyDownload(channel, key, count, time)
//    })
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//  case class ChannelTopKeyFlux(channel: String, key: String, flux: Long, time: Int)
//
//  // 分渠道的 top xxx 的流量
//  def createChannelTopKeyFlux(sql: String, writeUrl: String) = {
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val key = r.getString(1)
//      val flux = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelTopKeyFlux(channel, key, flux, time)
//    })
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//  case class ChannelRestime(channel: String, size: Long, restime: Long, time: Int)
//
//  // 下载的总大小、总耗时
//  def createChannelRestime(sql: String, writeUrl: String) = {
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val size = r.getLong(1)
//      val restime = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelRestime(channel, size, restime, time)
//    })
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//  case class ChannelStatus(channel: String, code: Int, count: Long, time: Int)
//
//  // 渠道、状态码、状态码个数
//  def createChannelStatus(sql: String, writeUrl: String) = {
//    val reginrdd = sqlContext.sql(sql).map(r => {
//      val channel = r.getString(0)
//      val code = r.getInt(1)
//      val count = r.getLong(2)
//      val time = r.getInt(3)
//      ChannelStatus(channel, code, count, time)
//    })
//    (sqlContext.createDataFrame(reginrdd).write, writeUrl, sql)
//  }
//
//  def analyseTable(writeParquetUrlPrefix: String, timeSuffix: String, check: Boolean,
//                   typeSqls: Seq[(String, String, String, String)]): Int = {
//    val needAnalyses = typeSqls.filterNot(typeSqlUrlTable => {
//      val value = check && alreadyAnalysed(s"$writeParquetUrlPrefix/${typeSqlUrlTable._1}/$timeSuffix")
//      info(s"check && alreadyAnalysed($writeParquetUrlPrefix/${typeSqlUrlTable._1}/$timeSuffix): $check $value")
//      value
//    })
//
//    if (needAnalyses.length > 0) {
//      needAnalyses.map(i => (i._3, i._4)).distinct.par.foreach(p => registerTable(p._1, p._2))
//
//      val tasks = needAnalyses.par.map(typeSql => {
//        val aType = typeSql._1
//        val sql = typeSql._2
//        val writeUrl = s"$writeParquetUrlPrefix/$aType/$timeSuffix"
//        deleteFile(writeUrl, true)
//        (aType: @switch) match {
//          case `channelRegionUsercountType` => createChannelRegionUsercount(sql, writeUrl)
//          case `channelSystemUsercountType` => createChannelSystemUsercount(sql, writeUrl)
//          case `channelDownloadType` => createChannelDownload(sql, writeUrl)
//          case `channelTopKeyDownloadType` => createChannelTopKeyDownload(sql, writeUrl)
//          case `channelTopKeyFluxType` => createChannelTopKeyFlux(sql, writeUrl)
//          case `channelRestimeType` => createChannelRestime(sql, writeUrl)
//          case `channelStatusType` => createChannelStatus(sql, writeUrl)
//        }
//      })
//
//      val msg = ArrayBuffer[String]()
//
//      tasks.par.foreach(w => {
//        val write = w._1
//        val writeUrl = w._2
//        val sql = w._3
//        info(s"开始汇总表: $writeUrl : ${new DateTime()}")
//        try {
//          write.parquet(writeUrl)
//          info(s"汇总表完成: $writeUrl : ${new DateTime()}")
//        } catch {
//          case e: Throwable => {
//            val s = s"  汇总表失败: $write $sql to $writeUrl : ${new DateTime()} \n" +
//              s" ${e} ${e.getCause} ${if (e.getCause != null) e.getCause.getCause} "
//            msg += s
//            warn(s)
//            e.printStackTrace()
//          }
//        }
//      })
//
//      info1(s"汇总表结束: $writeParquetUrlPrefix/*  */$timeSuffix : ${new DateTime()}, 失败信息: \n ${msg.mkString("\n")}")
//    }
//    needAnalyses.length
//  }
//
//  ////////     按每小时汇总
//  def hourParquetSuffix(subject: String) = s"$bucket/parquet/$subject/hour"
//
//  def createHourTable(domain: String, datetime: DateTime, check: Boolean = true) = {
//    val timeSuffix = datetime.toString("yyyy/MM/dd/HH")
//    val time = datetime.getMillis / 1000 / 3600 * 3600
//    val parquetOriginUrl = s"$bucket/parquet/${domain}/${timeSuffix}/*"
//    val writeParquetUrlPrefix = hourParquetSuffix(domain)
//    val tableName = s"ytyk_temp_hour_${time}"
//
//    info(s"开始汇总小时表: $parquetOriginUrl to $writeParquetUrlPrefix : $tableName  : ${new DateTime()}\n")
//
//
//    val typeSqls = Seq(
//      (channelRegionUsercountType, s" select channel, model, nation, province, city, count(DISTINCT user) as userCount, " +
//        s" ${time} as time from ${tableName} where nation = '中国' group by  channel, model, nation, province, city ",
//        parquetOriginUrl, tableName),
//
//      (channelSystemUsercountType, s" select channel, system, count(DISTINCT user) as userCount, ${time} as time " +
//        s" from ${tableName} where nation = '中国' group by  channel, system ", parquetOriginUrl, tableName),
//
//      // 带宽： 5 分钟流量 / 300 * 8 bps
//      // 小时带宽， 取小时内 5 分钟带宽的最大值
//      // 暂不做
//      // ChannelBandwith
//      // channel, bandwith, time
//
//      // 分渠道的总下载次数
//      (channelDownloadType, s" select channel, count(1) as count, ${time} as time " +
//        s" from ${tableName} where nation = '中国' group by  channel ", parquetOriginUrl, tableName),
//
//      //    // 分渠道的 top 500 的下载次数
//      //    select  channel, key, count from (
//      //      select channel, key, count,  row_number() over(partition by channel order by count desc) idx  from (
//      //      select channel, key, count(1) as count
//      //        from ytyk where nation = '中国'  group by  channel, key
//      //    ) a ) b where idx < 5 order by channel, count desc
//      (channelTopKeyDownloadType, s"select  channel, key, count, ${time} as time from ( " +
//        " select channel, key, count, row_number() over(partition by channel order by count desc) idx from ( " +
//        s" select channel, key, count(1) as count from ${tableName} where nation = '中国'  group by  channel, key" +
//        " ) a ) b where idx < 500 order by channel, count desc", parquetOriginUrl, tableName),
//
//      // 分渠道的 top 500 的流量
//      (channelTopKeyFluxType, s"select  channel, key, flux, ${time} as time from ( " +
//        " select channel, key, flux, row_number() over(partition by channel order by flux desc) idx from ( " +
//        s" select channel, key, sum(size) as flux from ${tableName} where nation = '中国'  group by  channel, key" +
//        " ) a ) b where idx < 500 order by channel, flux desc", parquetOriginUrl, tableName),
//
//      // 下载的总大小、总耗时
//      (channelRestimeType, s" select  channel, sum(size) as totalsize, sum(restime) as totalrestime, ${time} as time " +
//        s" from ${tableName} where nation = '中国' group by  channel ", parquetOriginUrl, tableName),
//
//      // 渠道、状态码、状态码个数
//      (channelStatusType, s" select channel, code, count(1), ${time} as time " +
//        s" from ${tableName} where nation = '中国' group by  channel, code ", parquetOriginUrl, tableName)
//    )
//
//    analyseTable(writeParquetUrlPrefix, timeSuffix, check, typeSqls)
//  }
//
//  /////////  按已汇总数据统计
//
//  def createTable(domain: String, fromDateType: String, time: Int, timeSuffix: String, writeParquetUrlPrefix: String,
//                  tableSuffix: String, check: Boolean) = {
//    val pPrefix = s"${bucket}/parquet/${domain}/${fromDateType}"
//
//    val typeSqlParquentTables = Seq(
//      (channelRegionUsercountType, s" select channel, model, nation, province, city, sum(userCount) as userCount, " +
//        s" ${time} as time from ${channelRegionUsercountType}${tableSuffix} group by channel, model, nation, province, city ",
//        s"$pPrefix/$channelRegionUsercountType/${timeSuffix}/*", s"$channelRegionUsercountType$tableSuffix"),
//
//      (channelSystemUsercountType, s" select  channel, system, sum(userCount) as userCount,  ${time} as time " +
//        s"from ${channelSystemUsercountType}${tableSuffix} group by  channel, system ",
//        s"$pPrefix/$channelSystemUsercountType/${timeSuffix}/*", s"$channelSystemUsercountType$tableSuffix"),
//
//      // 带宽： 5 分钟流量 / 300 * 8 bps
//      // 小时带宽， 取小时内 5 分钟带宽的最大值
//      // 暂不做
//      // ChannelBandwith
//      // channel, bandwith, time
//
//      // 分渠道的总下载次数
//      (channelDownloadType, s" select   channel, sum(count) as count,  ${time} as time " +
//        s"from ${channelDownloadType}${tableSuffix} group by  channel ",
//        s"$pPrefix/$channelDownloadType/${timeSuffix}/*", s"$channelDownloadType$tableSuffix"),
//
//      //    // 分渠道的 top 500 的下载次数
//      //        select  channel, key, count from (
//      //    select channel, key, count,  row_number() over(partition by channel order by count desc) idx  from (
//      //      select channel, key, sum(count) as count
//      //        from ChannelTopKeyDownload_ytyk_temp_day_1486944000  group by  channel, key
//      //    ) a ) b where idx < 5 order by channel, count desc
//      (channelTopKeyDownloadType, s"select  channel, key, count, ${time} as time from ( " +
//        " select channel, key, count, row_number() over(partition by channel order by count desc) idx from ( " +
//        s" select channel, key, sum(count) as count from ${channelTopKeyDownloadType}${tableSuffix} group by  channel, key" +
//        " ) a ) b where idx < 500 order by channel, count desc",
//        s"$pPrefix/$channelTopKeyDownloadType/${timeSuffix}/*", s"$channelTopKeyDownloadType$tableSuffix"),
//
//      // 分渠道的 top 500 的流量
//      (channelTopKeyFluxType, s"select  channel, key, flux, ${time} as time from ( " +
//        " select channel, key, flux, row_number() over(partition by channel order by flux desc) idx from ( " +
//        s" select channel, key, sum(flux) as flux from ${channelTopKeyFluxType}${tableSuffix} group by  channel, key" +
//        " ) a ) b where idx < 500 order by channel, flux desc",
//        s"$pPrefix/$channelTopKeyFluxType/${timeSuffix}/*", s"$channelTopKeyFluxType$tableSuffix"),
//
//      // 下载的总大小、总耗时
//      (channelRestimeType, s" select  channel, sum(size) as size, sum(restime) as restime, ${time} as time " +
//        s" from ${channelRestimeType}${tableSuffix} group by  channel ",
//        s"$pPrefix/$channelRestimeType/${timeSuffix}/*", s"$channelRestimeType$tableSuffix"),
//
//      // 渠道、状态码、状态码个数
//      (channelStatusType, s" select channel, code, sum(count) as count, ${time} as time " +
//        s" from ${channelStatusType}${tableSuffix} group by  channel, code ",
//        s"$pPrefix/$channelStatusType/${timeSuffix}/*", s"$channelStatusType$tableSuffix")
//    )
//
//    analyseTable(writeParquetUrlPrefix, timeSuffix, check, typeSqlParquentTables)
//  }
//
//  ////////     小时汇总数据汇总为天数据
//
//  def dayParquetSuffix(subject: String) = s"$bucket/parquet/$subject/day"
//
//  def createDayTable(domain: String, datetime: DateTime, check: Boolean = true) = {
//    val timeSuffix = datetime.toString("yyyy/MM/dd")
//    val time = datetime.getMillis / 1000 / 86400 * 86400
//    val writeParquetUrlPrefix = dayParquetSuffix(domain)
//
//    val dayTableSuffix = s"_ytyk_temp_day_${time}"
//
//    createTable(domain, "hour", time.toInt, timeSuffix, writeParquetUrlPrefix, dayTableSuffix, check)
//  }
//
//  ////////     天汇总数据汇总为月数据
//
//  def monthParquetSuffix(subject: String) = s"$bucket/parquet/$subject/month"
//
//  def createMonthTable(domain: String, datetime: DateTime, check: Boolean = true) = {
//    val timeSuffix = datetime.toString("yyyy/MM/dd")
//    val time = new DateTime(datetime.getYear, datetime.getMonthOfYear, 0).getMillis / 1000
//    val writeParquetUrlPrefix = monthParquetSuffix(domain)
//
//    val tableSuffix = s"_ytyk_month_day_${time}"
//
//    createTable(domain, "day", time.toInt, timeSuffix, writeParquetUrlPrefix, tableSuffix, check)
//  }
//
//
//  // =============================================================
//
//
//  case class Record(user: String, ip: String, nation: String, province: String, city: String,
//                    restime: Int, time: Long, code: Int, size: Long,
//                    system: String, channel: String, device: String, model: String, build: String,
//                    key: String, suffix: String)
//
//
//  val devicesM = sc.textFile(devices).map(x => {
//    val s = x.split(",");
//    (s(0), s(1))
//  }).collectAsMap()
//
//  def getChannelByDevice(device: String): String = {
//    devicesM.getOrElse(device, "OTHOER")
//  }
//
//  def stripChars(s: String, ch: String) = s filterNot (ch contains _)
//
//  // AndroidDownloadManager/5.1.1+(Linux;+U;+Android+5.1.1;+OPPO+R9+Plusm+A+Build/LMY47V)
//  // Dalvik/2.1.0+(Linux;+U;+Android+5.1.1;+NX529J+Build/LMY47V)
//  // Dalvik/2.1.0+(Linux;+U;+Android+5.1.1;+NX523J_V1+Build/LMY47V)
//  // Dalvik/2.1.0+(Linux;+U;+Android+6.0.1;+vivo+Y55A+Build/MMB29M)
//  // AndroidDownloadManager/5.1+(Linux;+U;+Android+5.1;+OPPO+R9m+Build/LMY47I)
//  // ua 也包含其它字符
//  // -
//  // Java/1.7.0_09
//  // Go-http-client/1.1
//  // VAYXXLWZIKRFDGFHPOXDNHJTDLTNBTV
//  // ("Android 6.0.1", "vivo Y55A", "Build/MMB29M")
//  //
//  //  val firm = driver._1    Android 5.1
//  //  val device = driver._2   OPPO R9m
//  //  val rom = driver._3       Build/LMY47I
//  //  ---------------------
//  //  system Android 5.1
//  //  device oppo
//  //  model A59m
//  //  build LMY47
//  // 27.129.192.99 - 96 [03/Jan/2017:23:30:37 +0800] "GET http://7xna64.com2.z0.glb.qiniucdn.com/FqiqaB9Dgea0EluvDtfS9RPMnIll.jpg?imageView2/2/w/1080/h/1920&e=1472744999&token=Q-hCY0VbL4F6NTX3TgRvE_T3vcpNEo2Gr3S9RA-b:gjgcZwEQh3IradE00dse88e8zww= HTTP/1.1" 401 608 "-" "Mozilla/5.0+(Linux;+U;+Android+6.0;+zh-cn;GIONEE-GN8002/GIONEE-GN8002+Build/IMM76D)+AppleWebKit534.30(KHTML,like+Gecko)Version/4.0+Mobile+Safari/534.30+Id/AAC3010D557B0FC202D98648E58A517C+RV/5.0.16"
//  def parseUa(ua: String): (String, String, String, String, String) = {
//    try {
//      val uas = ua.split(";")
//
//      //Linux;+U;+Android+5.1.1;+zh-cn;GiONEE-M3S/M3S+Build/IMM76D
//      //remove +/-/space chars from  "+Android+5.1.1"
//      var system = stripChars(uas(2), "'\"+- ")
//      if (!"""(Android|Xhrome)[0-9.]*""".r.unapplySeq(system).isDefined) {
//        system = "Unknown"
//      }
//
//      //get GiONEE-M3S/M3S+Build/IMM76D
//      //+OPPO+R"
//      //NX531J Build/MMB29M
//      //+HTC+D816w+Build/MRA58K
//      val deviceModelBuild = uas.last.trim()
//      val deviceModelBuilds = deviceModelBuild.replaceAll("[+-]", " ").replaceAll("\\s{2,}", " ").trim.split(' ')
//      val device = deviceModelBuilds(0)
//
//      var model = device
//      var build = ""
//
//      //NX531J Build/MMB29M
//      if (deviceModelBuilds.length > 1 && !deviceModelBuilds(1).contains("Build")) {
//        val end = deviceModelBuilds.length - 1
//        model = deviceModelBuilds.slice(1, end).mkString("+")
//      }
//
//      if (deviceModelBuild.contains("Build")) {
//        build = deviceModelBuild.split("Build/").last
//      }
//
//      val channel = getChannelByDevice(device)
//
//      return (system, channel, device, model, build)
//    } catch {
//      case e: Exception => {
//        info(s"wrong user-agent==>:", ua)
//        return ("Unknown", "Unknown", "Unknown", "Unknown", "Unknown")
//      }
//    }
//  }
//
//
//  def parseToDate(dateParser: DateTimeFormatter, line: String): Long = {
//    dateParser.parseDateTime(line.substring(line.indexOf("[") + 1, line.indexOf("]"))).getMillis / 1000
//  }
//
//  def parseRegion(ipParser: Locator, ip: String) = {
//    /**
//      *
//      * [中国, 河南, 新乡, ]
//      * [中国, 贵州, 黔南布依族苗族自治州, ]
//      * [中国, 广东, 中山, ]
//      * [中国, 安徽, 芜湖, ]
//      * [共享地址, 共享地址, , ]
//      *
//      **/
//    val ipinfo = ipParser.find(ip)
//    //   if ("共享地址".equalsIgnoreCase(ipinfo(0))) throw new Exception(s"${ip} is a '共享地址'")
//    (ipinfo.country, ipinfo.state, ipinfo.city)
//  }
//
//  def parseToKey(url: String) = {
//    // https://a 至少有 9 个字符
//    val l = url.indexOf("?", 9);
//    val end = if (l > 0) l else url.length()
//    url.substring(url.indexOf("/", 9) + 1, end)
//  }
//
//  def hash(sha1Digester: MessageDigest, s: String): String = {
//    sha1Digester.digest(s.getBytes).map("%02x".format(_)).mkString
//  }
//
//
//  def mixtureUser(sha1Digester: MessageDigest, ip: String, ua: String) = {
//    hash(sha1Digester, ip + ":" + ua)
//  }
//
//  def parseToSuffix(key: String) = {
//    val s = key.lastIndexOf('.')
//    if (s < 0) {
//      ""
//    } else {
//      key.substring(s, key.length)
//    }
//  }
//
//
//  // 106.18.21.156 - 282 [03/Jan/2017:23:30:14 +0800] "GET http://7xna64.com2.z0.glb.qiniucdn.com/Fjm_mLtcPN3DbTtLpywOmX5gq9cl.jpg?imageView2/2/w/1080/h/1920&e=1483545599&token=Q-hCY0VbL4F6NTX3TgRvE_T3vcpNEo2Gr3S9RA-b:ffDUURujc65VJLj1mKdGDMOrhIg= HTTP/1.1" 200 478114 "-" "AndroidDownloadManager/5.1+(Linux;+U;+Android+5.1;+OPPO+R9m+Build/LMY47I)"
//  // 139.148.121.96 - 248 [03/Jan/2017:23:30:11 +0800] "GET http://7xna64.com2.z0.glb.qiniucdn.com/FiU3bxGjI6PutwVphDQQihBgP0uw.jpg?imageView2/2/w/1080/h/1920&e=1483545599&token=Q-hCY0VbL4F6NTX3TgRvE_T3vcpNEo2Gr3S9RA-b:1wKdyBO_iYMQh7_MBqGcifYQX50= HTTP/1.1" 200 552867 "-" "AndroidDownloadManager/5.1.1+(Linux;+U;+Android+5.1.1;+OPPO+R9+Plusm+A+Build/LMY47V)"
//  // 220.178.4.219 - 1 [03/Jan/2017:23:30:35 +0800] "GET http://7xna64.com2.z0.glb.qiniucdn.com/FiwmSuSIuu981zLWENSCOJvIoj2P.jpg?imageView2/2/w/1080/h/1920&e=1483592399&token=Q-hCY0VbL4F6NTX3TgRvE_T3vcpNEo2Gr3S9RA-b:vsvEgQcb8-cU3BDLNp6sLCG72DI= HTTP/1.1" 200 456693 "-" "Dalvik/2.1.0+(Linux;+U;+Android+5.1.1;+NX529J+Build/LMY47V)"
//  def parse(dateParser: DateTimeFormatter, sha1Digester: MessageDigest, ipParser: Locator, line: String): Record = {
//    try {
//      val as = line.split(" ")
//
//      val ip = as(0)
//      val restime = as(2).toInt
//      val time = parseToDate(dateParser, line)
//
//      val code = as(8).toInt
//      val size = as(9).toLong
//
//      val pattern = new Regex("Linux.*U[^)]*")
//      val ua = (pattern findAllIn line).mkString
//
//      val region = parseRegion(ipParser, ip)
//      val nation = region._1
//      val province = region._2
//      val city = region._3
//
//      val driver = parseUa(ua)
//      val system = driver._1
//      val channel = driver._2
//      val device = driver._3
//      val model = driver._4
//      val build = driver._5
//
//      val user = mixtureUser(sha1Digester, ip, ua)
//
//      val key = parseToKey(as(6))
//
//      val suffix = parseToSuffix(key)
//
//      return Record(user, ip, nation, province, city,
//        restime, time, code, size,
//        system, channel, device, model, build, key, suffix)
//    } catch {
//      case e: Exception => {
//        e.printStackTrace()
//        info1(s"wrong line: ${line}")
//        return null
//      }
//    }
//  }
//
//  def parseLog(url: String, markUrl: String) = {
//    info(url, markUrl)
//    val rdd = sc.textFile(url)
//    val infordd = rdd.mapPartitions(iter => {
//      val ipParser = Locator.loadFromNet(ipfile)
//      val dateParser = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
//      val sha1Digester = MessageDigest.getInstance("SHA1")
//      iter.map(parse(dateParser, sha1Digester, ipParser, _)).filter(_ != null)
//    })
//
//    info("==========+++++++++++++")
//    // 本地运行此行代码,报 612 ,原因待查。Zeppelin 上正常
//    sqlContext.createDataFrame(infordd).write.parquet(markUrl)
//    info("++++++++++=+++++++++++++")
//
//    //    val df = sqlContext.createDataFrame(infordd)
//    //    df.registerTempTable("fusion")
//    //    df.sqlContext.sql("select * from fusion limit 10").collect().foreach(info)
//    //    df.sqlContext.sql("select count(1) from fusion").collect().foreach(info)
//  }
//
//  def parseAndStore(domain: String, datetime: DateTime, check: Boolean = true) = {
//    info("start at: ", new DateTime())
//
//    val date = datetime.toString("yyyy/MM/dd/HH")
//
//    val r = s"$bucket/v2/${domain}_${datetime.toString("yyyy-MM-dd-HH")}*"
//    val w = s"$bucket/parquet/$domain/$date/"
//
//    if (check && fs.exists(new Path(s"${w}_SUCCESS"))) {
//      info(s"$w/_SUCCESS is exists, ${r} has been parsed.: ${new DateTime()}")
//      false
//    } else {
//      // 删除上次生成文件，以及成功标识符
//      info(s" ==== start to Parse $r To parquet: $w : ${new DateTime()}====")
//      deleteFile(w)
//      parseLog(r, w)
//      info(s" ==== Parsed over $r To parquet: $w : ${new DateTime()}====")
//      true
//    }
//  }
//
//
//  def execHour(domain: String, dateTime: DateTime, msg: ArrayBuffer[String], check: Boolean = true) = {
//    info(s"\n\n========================  ${dateTime}")
//    try {
//      info("parseAndStore: ", domain, dateTime, check)
//      parseAndStore(domain, dateTime, check)
//      info("createHourTable: ", domain, dateTime, check)
//      createHourTable(domain, dateTime, check)
//    } catch {
//      case e: SparkException => {
//        if (e.getCause != null) {
//          msg += s" ${domain}, ${dateTime}, ${e}: ${e.getCause.getMessage}: ${new DateTime()}"
//        } else {
//          msg += s" ${domain}, ${dateTime}, ${e.getMessage}: ${new DateTime()}"
//        }
//        throw e
//      }
//      case e: Throwable => {
//        msg += s" ${domain}, ${dateTime}, ${e.getMessage}: ${new DateTime()}"
//        throw e
//      }
//    }
//  }
//
//
//  // redoHour(Seq(new DateTime(2017, 2, 13, 19, 5), new DateTime(2017, 2, 13, 20, 5)))
//  def execHours(domain: String, datetimes: Seq[DateTime], check: Boolean = true) = {
//    val msg = ArrayBuffer[String]()
//    var ex: Throwable = null
//
//    datetimes.par.foreach(dateTime => {
//      try {
//        execHour(domain, dateTime, msg, check)
//      } catch {
//        case e: Throwable => {
//          if (ex == null) {
//            ex = e
//          }
//        }
//      }
//    })
//
//    if (msg.length > 0) {
//      val s = "\n parsing or create diff table failed: \n " + msg.mkString(",\n ")
//      if (ex != null) {
//        ex.printStackTrace()
//        throw new Exception(s, ex)
//      }
//    }
//  }
//
//  def cleanHour(domain: String, datetimes: Seq[DateTime]) = {
//    val msg = ArrayBuffer[String]()
//
//    try {
//      datetimes.par.foreach(datetime => {
//        info("clean parquet parseAndStore: ", domain, datetime)
//        val date = datetime.toString("yyyy/MM/dd/HH")
//        val w = s"$bucket/parquet/$domain/$date/"
//        deleteFile(s"${w}_SUCCESS", false)
//
//        info("clean hour table: ", domain, datetime)
//        val timeSuffix = datetime.toString("yyyy/MM/dd/HH")
//        val writeParquetUrlPrefix = hourParquetSuffix(domain)
//
//        types.par.foreach(t => {
//          val f = s"$writeParquetUrlPrefix/$t/$timeSuffix/_SUCCESS"
//          deleteFile(f, false)
//        })
//      })
//    } catch {
//      case e: SparkException => {
//        if (e.getCause != null) {
//          msg += s" ${domain}, ${e}: ${e.getCause.getMessage}: ${new DateTime()}"
//        } else {
//          msg += s" ${domain},  ${e.getMessage}: ${new DateTime()}"
//        }
//        // throw e
//      }
//      case e: Throwable => {
//        msg += s" ${domain},  ${e.getMessage}: ${new DateTime()}"
//        // throw e
//      }
//    }
//  }
//
//
//  def cleanDay(domain: String, datetimes: Seq[DateTime]) = {
//    val msg = ArrayBuffer[String]()
//
//    try {
//      datetimes.par.foreach(datetime => {
//        info("clean day table: ", domain, datetime)
//
//        val timeSuffix = datetime.toString("yyyy/MM/dd")
//        val writeParquetUrlPrefix = dayParquetSuffix(domain)
//
//        types.par.foreach(t => {
//          val f = s"$writeParquetUrlPrefix/$t/$timeSuffix/_SUCCESS"
//          deleteFile(f, false)
//        })
//      })
//    } catch {
//      case e: SparkException => {
//        if (e.getCause != null) {
//          msg += s" ${domain}, ${e}: ${e.getCause.getMessage}: ${new DateTime()}"
//        } else {
//          msg += s" ${domain},  ${e.getMessage}: ${new DateTime()}"
//        }
//        // throw e
//      }
//      case e: Throwable => {
//        msg += s" ${domain},  ${e.getMessage}: ${new DateTime()}"
//        // throw e
//      }
//    }
//  }
//
//
//  def cleanMonth(domain: String, datetimes: Seq[DateTime]) = {
//    val msg = ArrayBuffer[String]()
//
//    try {
//      datetimes.par.foreach(datetime => {
//        info("clean month table: ", domain, datetime)
//
//        val timeSuffix = datetime.toString("yyyy/MM/dd")
//        val writeParquetUrlPrefix = monthParquetSuffix(domain)
//
//        types.par.foreach(t => {
//          val f = s"$writeParquetUrlPrefix/$t/$timeSuffix/_SUCCESS"
//          deleteFile(f, false)
//        })
//      })
//    } catch {
//      case e: SparkException => {
//        if (e.getCause != null) {
//          msg += s" ${domain}, ${e}: ${e.getCause.getMessage}: ${new DateTime()}"
//        } else {
//          msg += s" ${domain},  ${e.getMessage}: ${new DateTime()}"
//        }
//        // throw e
//      }
//      case e: Throwable => {
//        msg += s" ${domain},  ${e.getMessage}: ${new DateTime()}"
//        // throw e
//      }
//    }
//  }
//
//
//  def start(): Unit = {
//    val domain = "7xna64.com2.z0.glb.qiniucdn.com"
//
//    try {
//      val now = new DateTime()
//      if ((now.getDayOfMonth == 3 && (now.getHourOfDay == 0 || now.getHourOfDay == 2)) ||
//        (now.getDayOfMonth == 4 && (now.getHourOfDay == 0 || now.getHourOfDay == 2))) {
//        info("createMonthTable: ", domain, now.plusDays(-10))
//        // 上个月,具体天不重要
//        createMonthTable(domain, now.plusDays(-10))
//      }
//    } catch {
//      case e: Throwable => {
//        e.printStackTrace()
//        // throw e
//      }
//    }
//
//    try {
//      val now = new DateTime()
//      if (now.getHourOfDay == 16 || now.getHourOfDay == 18) {
//        info("createDayTable: ", domain, now.plusHours(-36))
//        // 昨天,具体小时不重要
//        // createDayTable(domain, now.plusHours(-24 - 10))
//      }
//      if (now.getHourOfDay == 20 || now.getHourOfDay == 22) {
//        info("createDayTable: ", domain, now.plusHours(-36))
//        // 前天,具体小时不重要
//        // createDayTable(domain, now.plusHours(-48 - 10))
//      }
//    } catch {
//      case e: Throwable => {
//        e.printStackTrace()
//        // throw e
//      }
//    }
//
//    try {
//      val now = new DateTime()
//      val datetimes = (8).to(19).map(j => {
//        val i = -j
//        info(s"\n\n============ ${i}")
//        now.plusHours(i)
//      })
//      datetimes.grouped(6).foreach(v => {
//        v.foreach(info)
//        try {
//          execHours(domain, v, true)
//        } catch {
//          case e: Throwable => {
//            e.printStackTrace()
//            // throw e
//          }
//        }
//        info("task 'start' have been done")
//      })
//    } catch {
//      case e: Throwable => {
//        e.printStackTrace()
//        // throw e
//      }
//    }
//    info("done")
//  }
//
//  val line = new DateTime(2017, 2, 1, 0, 30)
//  val line2 = new DateTime(2017, 2, 15, 0, 0)
//
//  def start2() = {
//    val domain = "7xna64.com2.z0.glb.qiniucdn.com"
//    val now = new DateTime()
//
//    val datetimes = (19).to(400).map(i => {
//      now.plusHours(-i)
//    }).filter(d => d.getMillis > line.getMillis && d.getMillis < line2.getMillis)
//
//    datetimes.grouped(6).foreach(v => {
//      v.foreach(info)
//      try {
//        execHours(domain, v, true)
//      } catch {
//        case e: Throwable => {
//          e.printStackTrace()
//          // throw e
//        }
//      }
//    })
//    info("done")
//  }
//
//  def start3() = {
//    val domain = "7xna64.com2.z0.glb.qiniucdn.com"
//    val now = new DateTime()
//    info(now.plusHours(-430))
//    val datetimes = (-430).to(-18).map(i => {
//      now.plusHours(i)
//    }).filter(d => d.getMillis > line.getMillis && d.getMillis < line2.getMillis)
//
//    datetimes.grouped(6).foreach(v => {
//      v.foreach(info)
//      try {
//        execHours(domain, v, true)
//      } catch {
//        case e: Throwable => {
//          e.printStackTrace()
//          // throw e
//        }
//      }
//    })
//    info("done")
//  }
//
//  // Zeppelin 不需要 main 方法块, 将 start 方法和 info 平级,直接调用
//  def main(args: Array[String]): Unit = {
//    info("====+++  start all  at ", new DateTime())
//    //    start()
//    //    redoHour(Seq(new DateTime(2017, 2, 13, 19, 5), new DateTime(2017, 2, 13, 20, 5)))
//    //    info("====+++  end all  at ", new DateTime())
//    //    val domain = "7xna64.com2.z0.glb.qiniucdn.com"
//    //    createDayTable(domain, new DateTime(2017, 2, 14, 1, 5))
//  }
//
//
//}
