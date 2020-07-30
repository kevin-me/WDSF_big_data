package com.kevin.stopstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

class MonitorStop(ssc: StreamingContext) extends Runnable {

  override def run(): Unit = {

    //获取hdfs分布式文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "hadoop")

    while (true) {
      try
        //主线程每隔5S秒钟，执行一次
        Thread.sleep(5000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      //获取StreamingContext程序的运行的状态
      val state: StreamingContextState = ssc.getState

      //判断hdfs上面某个目录是否存在，如果存在
      val bool: Boolean = fs.exists(new Path("hdfs://node01:8020/stopSpark"))
      if (bool) {
        //判断streamingContext程序的运行状态为ACTIVE,就进行优雅的停机
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
