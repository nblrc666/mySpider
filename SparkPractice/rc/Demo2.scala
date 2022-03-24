package com.rc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo2")
    val sc = new SparkContext(conf)

    val RDD: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val RDD2: RDD[Int] = RDD.mapPartitions(x => x.map(_ * 2))
    RDD2.collect().foreach(println)
    sc.stop()

  }

}
