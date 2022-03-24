package com.ipHomeWork

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection,DriverManager, PreparedStatement}

object HomeWork01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("HomeWork01")
    val sc = new SparkContext(conf)

    val ipInfo: RDD[String] = sc.textFile("D:\\java大数据\\spark\\04sparkIP归属地\\ip.txt")

    /**
     * 1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
     * 1.0.8.0|1.0.15.255|16779264|16781311|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
     * 1.0.32.0|1.0.63.255|16785408|16793599|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
     * 1.1.0.0|1.1.0.255|16842752|16843007|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
     */
    val ipInfoSpilt: RDD[(String, String, String)] = ipInfo.map(x => {
      val res1 = x.split("\\|")
      val url1 = res1(2)
      val url2 = res1(3)
      val sheng = res1(6)
      (url1, url2, sheng)
    })
    //    ipInfoSpilt.collect().foreach(println(_))
    //    (3658931170,3658931170,安徽)
    //使用广播变量，使用之前需要使用action将算子的数据提取到，最后通过driver广播到worker端
    val arrIpInfo = ipInfoSpilt.collect()
    //定义广播变量
    val broadcastIpInfo = sc.broadcast(arrIpInfo)
    //获取用户点击流日志，找到该用户属于哪个省并返回
    val proviceAndOne: RDD[(String, Int)] = sc.textFile("D:\\java大数据\\spark\\04sparkIP归属地\\access.log").map(line => {
      val fileds = line.split("\\|")
      val ip = fileds(1)
      //用户的ip
      val ipToLong = ip2Long(ip)
      // 得到用户的Long类型的IP
      val arrIpInfo = broadcastIpInfo.value
      // IP基础数据
      val index = binarySearch(arrIpInfo, ipToLong)
      //根据索引找到对应的省
      val provice = arrIpInfo(index)._3
      (provice,1)
    })
//    1
//    proviceAndOne.collect().foreach(println(_))
//    2
    val res: RDD[(String, Int)] = proviceAndOne.reduceByKey(_ + _)
//    val res1 = res.collect()
//    println(res1.toBuffer)
    res.foreachPartition(it => data2MySQL(it))
    sc.stop()
  }

  //将ip转为long类型
  def ip2Long(ip: String) = {
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //使用二分法查找指定的范围所属
  def binarySearch(arr: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = arr.length
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= arr(middle)._1.toLong) && (ip <= arr(middle)._2.toLong)) {
        return middle
      }
      if (ip < arr(middle)._1.toLong) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

  //将数据写入到数据库中
  def data2MySQL(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/world?characterEncoding=UTF-8", "root", "123456")
    //将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log(province,pro_num) VALUES (?, ?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if (pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
