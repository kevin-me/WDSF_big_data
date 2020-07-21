package com.kevin.ganhuo

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOnHBase {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("readHbase")

    val sc = new SparkContext(conf)
    //--创建Hbase的环境变量参数
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "spark_hbase")

    // 这种方式 推荐
    val hbaseContext = new HBaseContext(sc, hbaseConf)

    val scan = new Scan()

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf("spark_hbase"), scan)

    hbaseRDD.map(eachResult => {

      val rowkey = Bytes.toString(eachResult._1.get())
      val result: Result = eachResult._2
      //--查询出来的结果集存在 (ImmutableBytesWritable, Result)第二个元素
      //--获取行键
      val rowKey = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))

      println(rowKey + ":" + name + ":" + age)

    }).foreach(println)

    sc.stop()
  }

  /**
   * Error:scalac: missing or invalid dependency detected while loading class file 'HBaseContext.class'.
   * Could not access type Logging in package org.apache.spark,
   * because it (or its dependencies) are missing. Check your build definition for
   * missing or conflicting dependencies. (Re-run with `-Ylog-classpath` to see the problematic classpath.)
   * A full rebuild may help if 'HBaseContext.class' was compiled against an incompatible version of org.apache.spark.
   */

  // 解决这个报错
  //在工程下面创建package   org.apache.spark

 // 然后在这个package下面创建scala的trait    Logging。Logging的内容如下


}
