package com.kevin.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从socket当中接收数据，实现单词计数统计
  */
object SocketSource {

  def updateFunct (inputValue:Seq[Int],historyValue: Option[Int]) :Option[Int] = {
    val result: Int = inputValue.sum + historyValue.getOrElse(0)

    Some(result)

  }

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setAppName("socketCount").setMaster("local[4]")


    //获取程序的入口类 StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    //每隔1s钟去处理一次数据
    ssc.checkpoint("./streaming_state")


    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    val accumulateResult: DStream[(String, Int)] = socketTextStream.flatMap(x => x.split(" ")).map((_, 1)).updateStateByKey(updateFunct)

  //  val result: DStream[(String, Int)] = socketTextStream.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    accumulateResult.print()


    //启动流程序
    ssc.start()
    ssc.awaitTermination()





  }

}
