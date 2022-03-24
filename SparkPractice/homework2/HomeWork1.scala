package com.homework2

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object HomeWork1 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("HomeWork1").master("local[4]").getOrCreate()
    import session.implicits._
    val rulesLines: Dataset[String] = session.read.textFile("D:\\java大数据\\spark\\04sparkIP归属地\\ip.txt")
    val deptOneAndOne: Dataset[(Long, Long, String)] = rulesLines.map(line => {
      val fields: Array[String] = line.split("\\|")
      val startURL: Long = fields(2).toLong
      val endURL: Long = fields(3).toLong
      val province: String = fields(6)
      (startURL, endURL, province)
    })
    val ipRulesRdd: DataFrame = deptOneAndOne.toDF("startURL", "endURL", "province")


    val accessLines: Dataset[String] = session.read.textFile("D:\\java大数据\\spark\\04sparkIP归属地\\access.log")
    val result: Dataset[Long] = accessLines.map(log => {
      val fields: Array[String] = log.split("\\|")
      val ip: String = fields(1)
      val ipNum: Long = ip2Long(ip)
      ipNum
    })
    val DFResult: DataFrame = result.toDF("ipNum")

    //创建视图
    val table1: Unit = ipRulesRdd.createTempView("table1")
    val table2: Unit = DFResult.createTempView("table2")

    val sqlResult: DataFrame = session.sql("SELECT province,count(*) counts FROM table1 JOIN table2 ON (ipNum>=startURL AND ipNum<=endURL) GROUP BY province ORDER BY counts DESC")
    sqlResult.show()
    session.stop()

  }

  //将ip转为long类型
  def ip2Long(ip: String) = {
    val fragmentss: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragmentss.length) {
      ipNum = fragmentss(i).toLong | ipNum << 8L
    }
    ipNum
  }

}
