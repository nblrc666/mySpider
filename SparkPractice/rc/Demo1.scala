package com.rc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Demo1")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))

//    def mapFunction(num: Int): Int = {
//      num * 2
//    }

//    val mapRDD: RDD[Int] = rdd.map(mapFunction)
//    val mapRDD: RDD[Int] = rdd.map(_*2)
//    val mapRDD1: RDD[String] = rdd.map(_.toString)
    val mapRDD2: RDD[Unit] = rdd.map(
      num => {

      }
    )
//    mapRDD.collect().foreach(println)
//    println("============================")
//    mapRDD1.collect().foreach(println)
//    println("============================")
    mapRDD2.collect().foreach(println)

    sc.stop()

  }

}
