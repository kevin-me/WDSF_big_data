object SumList {

  def main(args: Array[String]): Unit = {
   // var list = ListBuffer[Int]()

//    for(i <- 1 to 100)
//      list ++=ListBuffer(i)
//    val list2 =list.toList
//       print("list:"+list2)
//    val res=list2.reduce((x,y) => x + y)
//      println("sum :")
//      println(res)

     var ss= 1::2::Nil

//    for (i <- 1 to 2)
//      println(ss)

  //  val list = List(1,2,3,4)

//    list.foreach(x=>println(x))

//    list.foreach(println(_))
//
//    val aa =list.map(x=>x*11)
//
//    aa.foreach(println)
//
//     list.map(_*10)


//    val list1= List("hadoop hive spark flink", "hbase spark")
//
//   val bbb= list1.flatMap(x=>x.split(" "))
//
//    bbb.foreach(println)


    val list=List(1,2,3,4,5,6,7,8,9,10)

    val sss= list.filter(_>5).map(_*4)

    sss.foreach(println)

      list.sorted
    val a = List("张三"->"男", "李四"->"女", "王五"->"男")

   val ssd= a.groupBy(_._2)

    for((k,v) <- ssd) println(k+" -> "+v)

  }

}
