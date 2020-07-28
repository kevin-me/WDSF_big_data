package com.kevin.higherSuanzi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TransformWordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("TransformWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val sortedStream: DStream[(String, Int)] = result.transform(rdd => {


      val sortedRdd: RDD[(String, Int)] = rdd.sortBy(_._2, false)

      val top3Rdd: Array[(String, Int)] = sortedRdd.take(3)

      top3Rdd.foreach(println)

      sortedRdd

    })

    sortedStream.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
