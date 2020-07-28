package com.kevin.higherSuanzi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, Seconds, State, StateSpec, StreamingContext, Time}


object MapWithStateWordCount {


  def main(args: Array[String]): Unit = {

    //设置日置等级
    Logger.getLogger("org").setLevel(Level.ERROR)

    //创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("mapWithState").setMaster("local[0]")

    //创建streamingContext
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    //创建初始值
    val initRdd: RDD[(String, Int)] = streamingContext.sparkContext.parallelize((List(("hadoop", 10), ("spark", 20))))


    val stateSpec = StateSpec.function((time: Time, key: String, currentValue: Option[Int], historyState: State[Int]) => {

      //当前批次结果与历史批次的结果累加
      val sumValue: Int = currentValue.getOrElse(0) + historyState.getOption().getOrElse(0)
      val output = (key, sumValue)


      if (!historyState.isTimingOut()) {
        historyState.update(sumValue)
      }

      Some(output)
      //给一个初始的结果initRDD
      //timeout: 当一个key超过这个时间没有接收到数据的时候，这个key以及对应的状态会被移除掉
    }).initialState(initRdd).timeout(Durations.seconds(5))

    //创建checkpoint
    streamingContext.checkpoint("hdfs://node01:8020/sc")

    //读取socket中的数据
    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("node01", 9999)

    //处理数据
    val processStream: DStream[(String, Int)] = socketTextStream.flatMap(x => x.split(" ")).map((_, 1))


    val result: MapWithStateDStream[String, Int, Int, (String, Int)] = processStream.mapWithState(stateSpec)

    result.stateSnapshots().print()

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}
