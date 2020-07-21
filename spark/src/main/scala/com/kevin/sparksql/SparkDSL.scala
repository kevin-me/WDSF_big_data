package com.kevin.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//需求：读取文本文件，配合样例类，实现表格的注册，使用dsl以及asl语法进行操作

//case class Person(id:String,name:String,age:Int)

object SparkDSL {

  def main(args: Array[String]): Unit = {




    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //获取到sparkContext
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")

    val personRDD: RDD[String] = context.textFile("file:///D:\\开课吧课程资料\\15、scala与spark课程资料\\2、spark课程\\spark_day04\\数据\\person.txt")


    val personArray: RDD[Array[String]] = personRDD.map(x => x.split(" "))


    val rddWithPersonSchema: RDD[Person] = personArray.map(x =>Person(x(0),x(1),x(2).toInt))

    //导入隐式转换的包  需要sparkSession这个对象
    import sparkSession.implicits._
    //获取到了personDF
    val personDF: DataFrame = rddWithPersonSchema.toDF


    //对数据进行操作
    personDF.printSchema()
    personDF.show()



    //查询name字段的值   select  name  from  table
    personDF.select("name").show()
    personDF.select($"name").show()

    //对age字段进行加1操作
    personDF.select($"age" + 1).show()

    //过滤大于30岁的人
    personDF.filter($"age" > 30).show

    //按照年龄进行分组
    personDF.groupBy($"age").count().show()

    personDF.groupBy("age").count().sort($"count".desc).show()


    println("分割线=================================================")

    //以上都是DSL语法的风格，接下来看sql风格
    personDF.createOrReplaceTempView("person")

    //使用SparkSession调用sql方法统计查询
    sparkSession.sql("select * from person").show
    sparkSession.sql("select name from person").show
    sparkSession.sql("select name,age from person").show
    sparkSession.sql("select * from person where age >30").show
    sparkSession.sql("select count(*) from person where age >30").show
    sparkSession.sql("select age,count(*) from person group by age").show
    sparkSession.sql("select age,count(*) as count from person group by age").show
    sparkSession.sql("select * from person order by age desc").show



    context.stop()
    sparkSession.stop()



  }


}
