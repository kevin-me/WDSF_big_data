package com.kevin.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}


object WorldCountWindow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("node01", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))

    //想要统计窗口当中的数据量的大小  每隔多长时间计算一次窗口   窗口的大小，窗口的滑动时间的间隔
//三个参数，第一个参数，对当前窗口的数据进行累加，第二个参数是窗口的大小，第三个参数窗口滑动时间的间隔
  val wordCounts: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
