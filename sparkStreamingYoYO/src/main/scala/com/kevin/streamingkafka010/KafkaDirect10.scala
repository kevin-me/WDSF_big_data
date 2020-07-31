package com.kevin.streamingkafka010

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}

object KafkaDirect10 {
  def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * 使用direct  基于0.10版本整合，
      * local[1]
      */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("kafkaDirectStreaming")

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(2))


    //使用010版本整合，消费kafka的数据

    val consistent: LocationStrategy = LocationStrategies.PreferConsistent


    /**
      * Subscribe 表示订阅某一个指定的topic
      * SubscribePattern  通过正则来进行匹配满足所有正则的topic的数据
      * assign  表示订阅某一个topic当中某些分区的数据
      * 订阅test这个topic里面的数据
      */

    /**
      * topics: Iterable[jl.String],
      * kafkaParams: collection.Map[String, Object]
      *
      *
      * topics: Iterable[jl.String],
      * kafkaParams: collection.Map[String, Object],
      * offsets: collection.Map[TopicPartition, Long]
      *
      */

    //2、使用direct接受kafka数据
    //准备配置
    val topics =Set("test")
    val kafkaParams=Map(
      "bootstrap.servers" ->"node01:9092,node02:9092,node03:9092",
      "group.id" -> "KafkaDirect10",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false"  //使用10版本整合，自动提交offset的值到kafka的一个默认的topic里面去了

    )
    val consumerStrategy: ConsumerStrategy[String,String] = ConsumerStrategies.Subscribe[String,String](topics, kafkaParams)
    /**
      * ssc: StreamingContext,
      * locationStrategy: LocationStrategy,
      * consumerStrategy: ConsumerStrategy[K, V],
      * perPartitionConfig: PerPartitionConfig   //表示的是每个分区的配置属性
      */
    val kafakDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, consistent, consumerStrategy)

    kafakDStream.foreachRDD(eachRDD  =>{
      //value就是获取到了kafak的数据
      val dataRDD: RDD[String] = eachRDD.map(line => line.value())
      dataRDD.foreach(eachLine =>{
        println(eachLine)
      })
      //针对每一个rdd里面的数据都处理完了，开始手动提交offset  能不能讲offset提交到hbase里面去呢？？？
      val ranges: Array[OffsetRange] = eachRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      for(eachRanges <- ranges){
        println(eachRanges.fromOffset)
        println(eachRanges.untilOffset)
        println( eachRanges.partition)
        println(eachRanges.topic)
      }
      //通过kafakDStream来提交offset
      kafakDStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
