package com.homework


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}

import scala.collection.mutable

object Work03 {
  def main(args: Array[String]): Unit = {


    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Work03").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数

    val rdd: RDD[Array[String]] = sc.textFile("hdfs://192.168.137.10:8020/information.log").map(_.split(",", -1)).filter(_.length >= 85)
    val value: RDD[((String, String), Int)] = rdd.map(line => {
      val city: String = line(24)
      val pro: String = line(25)
      ((city, pro), 1)
    })
    val RDD: RDD[((String, String), Int)] = value.reduceByKey(_ + _)

    val RDD2: RDD[(String, (String, Int))] = RDD.map(x => {
      (x._1._1, (x._1._2, x._2))
    })

    val num: Int = RDD2.map(x => {
      x._1
    }).distinct().count().toInt

    RDD.partitionBy(new MyPartition(num)).saveAsTextFile("hdfs://192.168.137.10:8020/inforOutPut")

    spark.stop()
    sc.stop()

  }
}
class MyPartition(val count: Int) extends Partitioner {
  override def numPartitions: Int = count

  private var num = -1

  private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()
  override def getPartition(key: Any): Int = {
    val value: String = key.toString
    val str: String = value.substring(1, value.indexOf(","))
    println(str)
    if (map.contains(str)) {
      map.getOrElse(str, num)
    } else {
      num += 1
      map.put(str, num)
      num
    }
  }

}
