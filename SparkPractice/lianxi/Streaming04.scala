package com.lianxi

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.InetSocketAddress

object Streaming04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ssm")
    val ssc = new StreamingContext(conf, Seconds(5))

    val address = Seq(new InetSocketAddress("192.168.137.10",18888))
    val sterm: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK_SER_2)
    val line: DStream[String] = sterm.map(x => new String(x.event.getBody.array()))

    val ds: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1))
    val ds1: DStream[(String, Int)] = ds.reduceByKey(_ + _)

    ds1.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
