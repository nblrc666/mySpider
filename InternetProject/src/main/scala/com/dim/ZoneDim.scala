package com.dim

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ZoneDim {
  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("ZoneDim").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._
    // 接收参数
    var Array(inputPath, outputPath) = args
    val df: DataFrame = spark.read.parquet(inputPath)

    // 创建表
    val dim: Unit = df.createTempView("dim")

    // sql语句。
    val sql =
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end) as OriginalRequest,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) as ValidRequest,
        |sum(case when requestmode =1 and processnode =3 then 1 else 0 end) as AdvertisingRequest,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) as NumberOfBids,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and iswin=1 then 1 else 0 end) as NumberOfSuccessfulBids,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as ShowNumber,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as PageView,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as MediaDisplayNumber,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as MediaHits,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as Expense,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as Cost
        |from dim
        |group by
        |provincename,cityname
        |""".stripMargin
    val resDF: DataFrame = spark.sql(sql)
    val load: Config = ConfigFactory.load()



    val peo = new Properties()
    peo.setProperty("user",load.getString("jdbc.user"))
    peo.setProperty("driver",load.getString("jdbc.driver"))
    peo.setProperty("password",load.getString("jdbc.password"))

    resDF.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName2"),peo)

    spark.stop()
    sc.stop()
  }

}
