package com.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HomeWork1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HomeWork1")
    val sc = new SparkContext(conf)

    val fileData: RDD[String] = sc.textFile("src/main/data/ip.txt")
    //
    //    val urlData: RDD[String] = fileData.map(
    //      line => {
    //        val dates: Array[String] = line.split("\\|")
    //        dates(6) +"|"+dates(7)+","
    //      }
    //    )
    //    urlData.saveAsTextFile("D:\\java大数据\\spark\\3.13作业\\ip1.txt")
    val url2: RDD[((String, String), Int)] = fileData.map({
      fileData =>
        val strings: Array[String] = fileData.split("\\|")
        ((strings(6), strings(7)), 1)
    }).reduceByKey(_ + _).sortBy(_._2, false)
    val url3: RDD[(String, Iterable[(String, Int)])] = url2.map({
      url2 => (url2._1._1, (url2._1._2, url2._2))
    }).groupByKey()
    val url4: RDD[(String, Iterable[(String, Int)])] = url3.map({
      url3 => (url3._1, url3._2.take(1))
    })
    url4.collect().toBuffer.foreach(println)
    sc.stop()
  }
}
