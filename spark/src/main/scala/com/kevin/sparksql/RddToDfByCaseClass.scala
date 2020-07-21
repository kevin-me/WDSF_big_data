package com.kevin.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
//定义样例类
case class Student(id: String, name: String, age: Int)

object RddToDfByCaseClass {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("RddToDfByCaseClass").master("local[2]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    sc.setLogLevel("WARN")

    val textRdd: RDD[Array[String]] = sc.textFile("File:///C:\\Users\\Administrator\\Desktop\\review\\spark\\spark04\\数据\\person.txt").map(_.split(" "))

    val personRdd: RDD[Student] = textRdd.map(x => Student(x(0), x(1), x(2).toInt))

    import spark.implicits._

    val personDf: DataFrame = personRdd.toDF()

    personDf.show()

    personDf.printSchema()

  }

}
