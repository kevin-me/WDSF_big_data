package com.kevin.checkpoint


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object DriverHAWordCount {

  val checkpointPath = "hdfs://node01:8020/checkpointDir"

  def creatingFunc(): StreamingContext = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("DriverHAWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //设置checkpoint目录
    ssc.checkpoint(checkpointPath)

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc)

    //todo: 5、打印结果
    result.print()

    ssc

  }

  def updateFunc(currentValue: Seq[Int], historyValues: Option[Int]): Option[Int] = {

    val newValue: Int = currentValue.sum + historyValues.getOrElse(0)
    Some(newValue)
  }


  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointPath, creatingFunc _)


    ssc.start()
    ssc.awaitTermination()
  }

}
