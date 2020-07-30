package com.kevin.streamingkafak08

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaReceiver08 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("kafkaReceiver08").setMaster("local[1]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(2))
    val zkQuorum = "node01:2181,node02:2181,node03:2181"
    val groupid = "kafkaReceiver08"
    val topics = Map("test" -> 1)

    /**
      * ssc: StreamingContext,
      * zkQuorum: String,
      * groupId: String,
      * topics: Map[String, Int],
      */

    val receiverDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, zkQuorum, groupid, topics)
    //通过  _2  获取到kafka的数据
    val datas: DStream[String] = receiverDStream.map(x => x._2)

    val result: DStream[(String, Int)] = datas.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    streamingContext.start()

    streamingContext.awaitTermination()


  }


}
