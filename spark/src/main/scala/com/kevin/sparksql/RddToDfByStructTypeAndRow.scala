package com.kevin.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RddToDfByStructTypeAndRow {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("structType").master("local[2]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    sc.setLogLevel("WARN")

    val datas: RDD[Array[String]] = sc.textFile("File:///C:\\Users\\Administrator\\Desktop\\review\\spark\\spark04\\数据\\person.txt").map(_.split(" "))

    val rowRDD: RDD[Row] = datas.map(x => Row(x(0),x(1),x(2).toInt))

    val  schema = StructType(StructField("id",StringType)::StructField("name",StringType)::StructField("age",IntegerType)::Nil)

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)

    dataFrame.printSchema()

    dataFrame.show()

  }

}
