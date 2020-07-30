package com.kevin.streamingkafak08

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
/**
  * sparkStreaming使用kafka 0.8API基于Direct直连来接受消息
  * spark direct API接收kafka消息，从而不需要经过zookeeper，直接从broker上获取信息。
  */
object KafkaDirect08 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //1、创建StreamingContext对象
    val sparkConf= new SparkConf()
      .setAppName("KafkaDirect08")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    //2、接受kafka数据
    val  kafkaParams=Map(
      "metadata.broker.list"->"node01:9092,node02:9092,node03:9092",
      "group.id" -> "KafkaDirect08"
    )
    val topics=Set("test")
   // KafkaUtils.createStream( )  //使用receiver这种方式来进行消费，offset保存在zk当中，效率太低  receiver线程与处理数据线程不是同一个线程，造成OOM的问题，数据积压的问题
    // createDirectStream 使用direct方式来进行消费，offset保存在kafka的comsumer_offset这个topic里面去了  接受数据与处理数据的线程同一个线程，可以支持数据的背压机制
    //使用direct直连的方式接受数据
    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    val value: DStream[String] = kafkaDstream.map(x => x._1)
    value.print()
    //3、获取kafka的topic数据
    val data: DStream[String] = kafkaDstream.map(_._2)
    //4、单词计数
    val result: DStream[(String, Int)] = data.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
    //5、打印结果
    result.print()
    //6、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}