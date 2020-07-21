package com.kevin.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CSVOperateToMysql {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CSVOperateToMysql")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    val frame: DataFrame = sparkSession.read.format("csv").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").option("header", true).option("multiLine", true).load("file:///C:\\Users\\Administrator\\Desktop\\review\\spark\\spark04\\数据\\招聘数据")

    frame.createOrReplaceTempView("job_detail")

   // val prop = new Properties()

   // prop.put("user", "root")

   // prop.put("password", "123456")

    //frame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mydb?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "mydb.jobdetail_copy", prop)

    sparkSession.sql("select * from job_detail").show




  }

}
