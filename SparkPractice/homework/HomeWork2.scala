package com.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HomeWork2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HomeWork2")
    val sc = new SparkContext(conf)

    val fileData: RDD[String] = sc.textFile("D:\\java大数据\\spark\\3.13作业\\ip1.txt")

    val rowRDD: RDD[(String, Int)] = fileData.flatMap(_.split(",")).map(x=>(x,1)).reduceByKey((x, y)=>x+y)


//    rowRDD.collect().foreach(println)

    rowRDD.sortBy(_._1,ascending = false).foreach(println)


  }

}
