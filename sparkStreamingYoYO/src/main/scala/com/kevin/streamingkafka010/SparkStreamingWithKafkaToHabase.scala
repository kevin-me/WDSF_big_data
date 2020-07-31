package com.kevin.streamingkafka010


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWithKafkaToHabase {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingWithKafkaToHabase").setMaster("local[2]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val locationStrategy = LocationStrategies.PreferConsistent


    /**
     *
     * topics : scala.Iterable[java.lang.String]
     * kafkaParams : scala.collection.Map[scala.Predef.String, java.lang.Object]
     *
     * offsets : scala.collection.Map[org.apache.kafka.common.TopicPartition
     *
     */


    val topics = Set("test")
    val kafkaParams = Map("bootstrap.servers" -> "node01:9020,node02:9020,node03:9020", "group.id" -> "KafkaDirect10",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false" //使用10版本整合，自动提交offset的值到kafka的一个默认的topic里面去了
    )

    val topicPartition1 = new TopicPartition("test", 0)

    val topicPartition2 = new TopicPartition("test", 1)
    val topicPartition3 = new TopicPartition("test", 2)

    val myMap = Map(topicPartition1 -> 25560, topicPartition2 -> 56680, topicPartition3 -> 89956)

    /* if(hbaseOffsetExists ){
          val value: Any = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, myMap)
        }else{
          val consumerStrategy: ConsumerStrategy[String,String] = ConsumerStrategies.Subscribe[String,String](topics, kafkaParams)
        }*/
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    /**
     * ssc : org.apache.spark.streaming.StreamingContext,
     * locationStrategy : org.apache.spark.streaming.kafka010.LocationStrategy,
     * consumerStrategy : org.apache.spark.streaming.kafka010.ConsumerStrategy[K, V]
     */
    val kafkaDirect: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, locationStrategy, consumerStrategy)

    kafkaDirect.foreachRDD(eachRdd => {

      val line: RDD[String] = eachRdd.map(line => line.value())

      line.foreach(data => {
        print(data)
      })

      val offsetRanges: Array[OffsetRange] = kafkaDirect.asInstanceOf[HasOffsetRanges].offsetRanges

      offsetRanges.foreach(offset => {
        print(offset.topic)
        print(offset.partition)
        print(offset.fromOffset)
        print(offset.untilOffset)
      })
      //手动提交
      kafkaDirect.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)


    })


  }

}
