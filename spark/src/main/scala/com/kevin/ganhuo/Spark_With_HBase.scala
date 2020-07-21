package com.kevin.ganhuo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.RandomRowFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//第一种方式：通过newAPIHadoopRDD来进行实现
object Spark_With_HBase {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_With_HBase").setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)

    val hBaseConf: Configuration = HBaseConfiguration.create()

    hBaseConf.set("hbase.zookeeper.quorum", "node01,node02,node03")

    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    hBaseConf.set(TableInputFormat.INPUT_TABLE, "spark_hbase")

    val scan = new Scan

    scan.setFilter(new RandomRowFilter(0.5f))

    hBaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    //newAPIHadoopRDD

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])


    hbaseRdd.foreach(x => {

      val row: Result = x._2

      val rowKey: String = Bytes.toString(row.getRow)

      val name: String = Bytes.toString(row.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))

      val age: String = Bytes.toString(row.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))

      println(rowKey + ":" + name + ":" + age)

    })

    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, "spark_hbase_out")

    val job: Job = Job.getInstance(hBaseConf)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    val outRdd: RDD[(ImmutableBytesWritable, Put)] = hbaseRdd.mapPartitions(eachPartitons => {
      eachPartitons.map(eachResult => {
        val result = eachResult._2
        //--获取行键
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
        val put = new Put(Bytes.toBytes(rowKey))
        val immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("info"),
          Bytes.toBytes("name"),
          Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("info"),
          Bytes.toBytes("age"),
          Bytes.toBytes(age))
        (immutableBytesWritable, put)
      })
    })

    outRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sparkContext.stop()
  }
}