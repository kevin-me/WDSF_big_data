package com.kevin.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SocketStructStreaming {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("socketStreaming").setMaster("local[4]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val lines: DataFrame = spark.readStream.format("socket").option("host", "node01").option("port", 9999)
      .load()

    import spark.implicits._
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    val wordCounts: DataFrame = words.groupBy("value").count()

    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
