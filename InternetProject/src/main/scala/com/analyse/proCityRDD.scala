package com.analyse

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util
import java.util.Properties

object proCityRDD {
  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("proCityRDD").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    var Array(inputPath, outputPath) = args

    val line: RDD[String] = sc.textFile(inputPath)
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val proCityRDD: RDD[((String, String), Int)] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    })

    // 降维。
    val reduceRDD: RDD[((String, String), Int)] = proCityRDD.reduceByKey(_ + _)

    val rdd2: RDD[(String, (String, Int))] = reduceRDD.map(arr => {
      (arr._1._1, (arr._1._2, arr._2))
    })

    val num: Long = rdd2.map(x => {
      (x._1, 1)
    }).reduceByKey(_ + _).count()

    rdd2.partitionBy(new HashPartitioner(num.toInt)).saveAsTextFile(outputPath)

    spark.stop()
    sc.stop()

  }


}
