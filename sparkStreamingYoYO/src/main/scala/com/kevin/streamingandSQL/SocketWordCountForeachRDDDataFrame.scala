package com.kevin.streamingandSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}



object SocketWordCountForeachRDDDataFrame {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDDDataFrame").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01",9999)
    //todo: 4、对数据进行处理
    val words: DStream[String] = socketTextStream.flatMap(_.split(" "))
    words.foreachRDD(rdd => {
      val sparkSession: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._
      //将我们的数据转换成为DataFrame
      val dataFrame: DataFrame = rdd.toDF("word")
      //将DataFrame注册成为一张表
      dataFrame.createOrReplaceTempView("words")
      val result: DataFrame = sparkSession.sql("select word,count(1) from  words group by word")
      result.show()
    })

    ssc.start()
    ssc.awaitTermination()

  }



}
