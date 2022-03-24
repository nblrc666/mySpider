package com.homework

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object HomeWork3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HomeWork3")
    val sc = new SparkContext(conf)
    // 求出每个学科中最受老师欢迎的TOP3
    val rdd: RDD[String] = sc.textFile("src/main/data/teacher.log")
    val projectAndTeacher: RDD[((String, String), Int)] = rdd.map(line => {
      val teacherName: String = line.split("/")(3)
      val project: String = line.split("/")(2).split("[.]")(0)
      ((project, teacherName),1)
    })
    val reduced: RDD[((String, String), Int)] = projectAndTeacher.reduceByKey(_+_)
    val projectReduced: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    val result= projectReduced.mapValues(_.toList.sortBy(_._2).reverse.take(3))
   println("每个学科中最受老师欢迎的TOP3"+"\n"+result.collect().toBuffer)
    println("=========================================")
    //在所有的老师中求出最受欢迎的老师Top3
    val rdd1: RDD[String] = rdd.map(rdd => {
      ((rdd.split("360.cn/")) (1))
    }
    )
    val rdd2: Array[(String, Int)] = rdd1.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(3)
    println("在所有的老师中求出最受欢迎的老师Top3"+"\n" + rdd2.toBuffer)
    sc.stop()
  }

}
