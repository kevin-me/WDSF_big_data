package com.kevin.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming._


object MapWithStateWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("MapWithStateWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))


    ssc.checkpoint("./mapWithState")


    val initRDD: RDD[(String, Int)] = ssc.sparkContext.parallelize((List(("hadoop",10),("spark",20))))



    val wordAndOne: DStream[(String, Int)] = ssc.socketTextStream("node01", 9999).flatMap(x => x.split(" ")).map((_, 1))

    val stateSpec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function((time: Time, key: String, currentValue: Option[Int], historyState: State[Int]) => {
      //累加的结果
      val sumValue: Int = currentValue.getOrElse(0) + historyState.getOption().getOrElse(0)

      val output = (key, sumValue)
      //更新数据的历史累加的状态值
      if (!historyState.isTimingOut()) {
        historyState.update(sumValue)
      }
      Some(output)
      //通过initialState 来设置起始的数据值  通过timeout来设置超时的时间值
    }).initialState(initRDD).timeout(Durations.seconds(2))


    val result: MapWithStateDStream[String, Int, Int, (String, Int)] = wordAndOne.mapWithState(stateSpec)

    result.stateSnapshots().print()  //打印最终的结果只

    ssc.start()
    ssc.awaitTermination()

  }
}
