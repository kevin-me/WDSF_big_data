package com.kevin.streamingkafka010

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 1.0 方式 直连
 */
object SparkStreamingWithKafka {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingWithKafka").setMaster("local[2]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    /**
     * LocationStrategies.PreferConsistent
     */
    val consistent: LocationStrategy = LocationStrategies.PreferConsistent

    /**
     * Subscribe()
     */

    /**
     * topics : scala.Iterable[java.lang.String],
     * kafkaParams : scala.collection.Map[scala.Predef.String, java.lang.Object]
     *
     *
     * topics: Iterable[jl.String],
     * kafkaParams: collection.Map[String, Object],
     * offsets: collection.Map[TopicPartition, Long]
     */

    val topics = Set("test")

    val kafkaParams = Map(
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "group.id" -> "KafkaDirect10",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false" //使用10版本整合，自动提交offset的值到kafka的一个默认的topic里面去了
    )

    /**
     * Subscribe 表示订阅某一个指定的topic
     * SubscribePattern  通过正则来进行匹配满足所有正则的topic的数据
     * assign  表示订阅某一个topic当中某些分区的数据
     * 订阅test这个topic里面的数据
     */

    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)


    //使用010版本整合，消费kafka的数据

    /**
     * * ssc : org.apache.spark.streaming.StreamingContext,
     * locationStrategy : org.apache.spark.streaming.kafka010.LocationStrategy,
     * consumerStrategy : org.apache.spark.streaming.kafka010.ConsumerStrategy[K, V]
     */

    val kafakDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, consistent, consumerStrategy)


    kafakDStream.foreachRDD(eachRdd => {

      //value就是获取到了kafak的数据
      val line: RDD[String] = eachRdd.map(line => line.value())

      line.foreach(eachLine => print(eachLine))


      //针对每一个rdd里面的数据都处理完了，开始手动提交offset  能不能讲offset提交到hbase里面去呢？？？

      val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges

      for (offRanges <- offsetRanges) {

        println(offRanges.fromOffset)
        println(offRanges.partition)
        println(offRanges.partition)
        println(offRanges.untilOffset)

      }
      //通过kafakDStream来提交offset
      kafakDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }


}
