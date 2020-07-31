package com.kevin.StructStreaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 读取socket当中的数据，实现数据统计
 */
object SocketStreaming {

  def main(args: Array[String]): Unit = {

    //设置日志等级
    Logger.getLogger("org").setLevel(Level.ERROR)


    //创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("structStreaming").setMaster("local[4]")

    //创建sparksession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val dataFrame: DataFrame = spark.readStream.format("socket").option("host", "node01").option("port", "9999").load()

    import spark.implicits._

    val words: Dataset[String] = dataFrame.as[String].flatMap(_.split(" "))


    val wordCounts: DataFrame = words.groupBy("value").count()

    val query: StreamingQuery = wordCounts.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

  }

}
