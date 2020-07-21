package com.kevin.sparksql

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUDF {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("file:///D:\\开课吧课程资料\\15、scala与spark课程资料\\2、spark课程\\spark_day04\\数据\\深圳链家二手房成交明细")

    frame.createOrReplaceTempView("house_sale")

    //自定义udf函数，可以输入多个字段，输出一个字段，如果输入两个字段，就是UDF2  如果输入3个字段就是UDF3
    //自定义udf函数，判断房子的年份，如果不存在或者是未知，就直接给定默认值1990
    //如果要全局注册，可以定义一个函数也可以
    session.udf.register("house_udf",new UDF1[String,String] {
      private val pattern: Pattern = Pattern.compile("^[0-9]*$")

      override def call(input: String): String = {
        val matcher: Matcher = pattern.matcher(input)
        if(matcher.matches()){
          input
        }else{
          "1990"
        }


      }
    },DataTypes.StringType)


    session.sql("select house_udf(house_age) from house_sale limit 200").show()

    session.stop()


  }

}
