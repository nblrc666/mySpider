package com.lianxi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds,StreamingContext}

object Streaming02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val ssc =  new StreamingContext(conf,Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("D:\\Idproject\\com.rc.spark\\src\\main\\scala\\com\\lianxi\\Streaming02")
    val  lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.137.10",9999)
    def updateFunction(currValues:Seq[Int],preValue:Option[Int]): Option[Int] = {
      val currValueSum = currValues.sum
      Some(currValueSum + preValue.getOrElse(0))
    }
    val result = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunction)
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)
  }
}
