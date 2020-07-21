package com.kevin.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQLWithCSV {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    val frame: DataFrame = session.read.format("CSV")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss:ZZ")
      .option("header", true)
      .option("multiLine", true) //数据有可能换行了
      .load("file:///D:\\开课吧课程资料\\15、scala与spark课程资料\\2、spark课程\\spark_day04\\数据\\招聘数据")
    frame.createOrReplaceTempView("job_detail")



    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")

    frame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mydb?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "mydb.jobdetail_copy", prop)




  }

}
