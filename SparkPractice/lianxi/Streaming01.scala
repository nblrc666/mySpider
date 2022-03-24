package com.lianxi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming01")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("192.168.137.10", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
