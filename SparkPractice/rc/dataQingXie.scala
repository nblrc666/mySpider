package com.rc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object dataQingXie {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dataQingXie")
    val sc = new SparkContext(conf)
    val rulesLines: RDD[String] = sc.textFile("hdfs://192.168.137.33:8020/nullid")
    rulesLines.map(line => {
      val fields = line.split("[\t]")
      val id = fields(0)
      (id, 1)
    }).reduceByKey(_+_).collect().foreach(println(_))

  }

}
