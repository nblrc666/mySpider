package com.homework

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Work01 {
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
    val spark = SparkSession.builder().config(conf).appName("Work01").master("local[1]").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._
    //接收参数
    val Array(inputpath,outputpath) = args
    val rdd: RDD[Array[String]] = sc.textFile(inputpath).map(_.split(",", -1)).filter(_.length >= 85)
    val value: RDD[(String, Int)] = rdd.map(line => {
      val str: String = line(24)
      (str, 1)
    })
    val value1: RDD[(String, Int)] = value.reduceByKey(_+_).sortByKey()
     value1.saveAsTextFile(outputpath)


  }

}
