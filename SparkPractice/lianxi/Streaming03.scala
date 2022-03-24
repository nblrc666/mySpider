package com.lianxi

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



object Streaming03 {
  /**
   * Spark对Kafka两种连接方式之一（直连方式）
   * @param args
   */
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("Streaming03")
    var ssc = new StreamingContext(conf,Seconds(5))
    // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
    // 4.1.配置kafka相关参数
    val kafka = Map(
      "metadata.broker.list"->"master:9092,server01:9092,server02:9092",
      "group.id"->"kafka_Direct"
    )
    //定义topic
    val topics = Set("dahua")

    //直连方式
    val ds: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafka, topics)
    //获取topic中的数据
    val ds1: DStream[String] = ds.map(_._2)

    //去切分每一行，每一个单词计为1
    val ds2: DStream[(String, Int)] = ds1.flatMap(_.split(" ")).map((_, 1))
    //相同单词出现得次数累加
    val ds3: DStream[(String, Int)] = ds2.reduceByKey(_ + _)
    //通过output operations操作打印数据
    ds3.print()
    //开启流式计算
    ssc.start()
    // 阻塞一直运行
    ssc.awaitTermination()
  }
}

