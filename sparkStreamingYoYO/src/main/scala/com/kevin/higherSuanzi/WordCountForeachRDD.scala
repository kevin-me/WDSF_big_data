package com.kevin.higherSuanzi

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 将WordCount案例中得到的结果通过foreachRDD保存结果到mysql中
 */

object WordCountForeachRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("word count to Mysql ").setMaster("local[2]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val socketTextStream: ReceiverInputDStream[String] = streamingContext
      .socketTextStream("node01", 9999)


    val result: DStream[(String, Int)] = socketTextStream.flatMap(x => x.split(" ")).map((_, 1)).reduceByKey(_ + _)

    /**
     * 方式一 不可取   获取链接没有实现序列化
     */
    result.foreachRDD(rdd => {

      val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
      val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")

      rdd.foreach(record => {
        //这一块代码的执行是在executor端，需要进行网络传输，会出现task not serializable 异常
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.execute()
      })

      statement.close()
      conn.close()
    })


    /**
     * 方式二 不可取   会创建大量的链接
     */
    result.foreachRDD(rdd => {

      rdd.foreach(record => {

        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")
        //这一块代码的执行是在executor端，需要进行网络传输，会出现task not serializable 异常
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.execute()
        statement.close()
        conn.close()
      })

    })

    /**
     * 方式三 不可取   foreachPartition  每个分区 创建个链接  但不是 最优的
     */
    result.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")

        iter.foreach(record => {
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          statement.execute()

        })
        statement.close()
        conn.close()
      })
    })


    //todo: 方案四   最优解
    result.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")
        //关闭自动提交
        conn.setAutoCommit(false);
        iter.foreach(record => {
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          //添加到一个批次
          statement.addBatch()

        })
        //批量提交该分区所有数据
        statement.executeBatch()
        conn.commit()
        // 关闭资源
        statement.close()
        conn.close()
      })
    })

    //todo: 6、开启流式计算
    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
