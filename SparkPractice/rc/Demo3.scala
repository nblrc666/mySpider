package com.rc

import org.apache.spark.{SparkConf, SparkContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo3")
    val sc = new SparkContext(conf)

    val RDD = sc.parallelize(Array(1,2,3,4,5))
    val RDD2 = RDD.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))
    RDD2.collect().foreach(println)
    sc.stop()
  }

}
