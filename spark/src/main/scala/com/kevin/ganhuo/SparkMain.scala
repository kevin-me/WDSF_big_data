package com.kevin.ganhuo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class SalesRecord(val transactionId: String, val customerId: String,
                       val itemId: String,
                       val itemValue: Double) extends Serializable


object SparkMain {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("this define rdd")

    val sparkContext = new SparkContext(sparkConf)

    val salesRdd: RDD[String] = sparkContext.textFile("file:///C:\\Users\\Administrator\\Desktop\\review\\spark\\spark03\\spark_day03\\2、数据准备\\sales.txt")

    val salesRecordRDD: RDD[SalesRecord] = salesRdd.map(x => {

      val colValues: Array[String] = x.split(",")

      new SalesRecord(colValues(0), colValues(1), colValues(2), colValues(3).toDouble)
    })


    import com.kevin.ganhuo.CustomFunctions._

    val moneyRDD: RDD[Double] = salesRecordRDD.changeDatas
    println("customer RDD  API:" + salesRecordRDD.changeDatas.collect().toBuffer)


    //给rdd增加action算子
    val totalResult: Double = salesRecordRDD.getTotalValue
    println("total_result" + totalResult)

    //自定义RDD，将RDD转换成为新的RDD
    val resultCountRDD: CustomerRDD = salesRecordRDD.discount(0.8)

    println(resultCountRDD.collect().toBuffer)

    sparkContext.stop()


  }

}

