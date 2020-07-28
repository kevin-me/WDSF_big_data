package com.kevin.higherSuanzi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateBykeyWordCount {

  def updateFunc(currentValue: Seq[Int], historyValue: Option[Int]): Option[Int] = {
    val result: Int = currentValue.sum + historyValue.getOrElse(0)
    Some(result)
  }

  def main(args: Array[String]): Unit = {

    //创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("udapteStateBykey").setMaster("local[2]")

    // 创建streamingcontext
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    //设置checkpoint 的路径
    streamingContext.checkpoint("hdfs://node01:8020/ck")

    //接受socket 数据
    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("node01", 9999)

    //对数据处理
    val wordandoneStream: DStream[(String, Int)] = socketTextStream.flatMap(x => x.split(" ")).map((_, 1))

    //返回数据
    val result: DStream[(String, Int)] = wordandoneStream.updateStateByKey(updateFunc)

    result.print()

    streamingContext.start();

    streamingContext.awaitTermination()


  }

}
