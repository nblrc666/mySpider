package com.rc

import com.bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object LogToParquet02 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
        """
          |缺少参数
          |inputpath | outputpath
          |""".stripMargin)
      sys.exit()
    }
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("LogToParquet02").master("local[*]").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._
    //接收参数
    val Array(inputpath,outputpath) = args

    val rdd: RDD[Array[String]] = sc.textFile(inputpath).map(_.split(",", -1)).filter(_.length >= 85)

    val rddLogBean: RDD[LogBean] = rdd.map(LogBean(_))

    val df: DataFrame = spark.createDataFrame(rddLogBean)

    df.write.parquet(outputpath)

    spark.stop()
    sc.stop()

  }

}
