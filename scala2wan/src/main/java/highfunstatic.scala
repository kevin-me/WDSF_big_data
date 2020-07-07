object highfunstatic {


  def main(args: Array[String]): Unit = {

    //    var  list = List(1,2,4,5)
    //    list.foreach(x=>print(x))
    //
    //    var list2=list.map(_*10)
    //
    //    list2.foreach(print(_))


    //    val list = List("hadoop hive spark flink", "hbase spark")
    //
    //    val list2=list.flatMap(_.split(" "))
    //
    //    list2.foreach(println(_))

    //    val list=List(1,2,3,4,5,6,7,8,9,10)
    //
    //    list.filter(_>5).map(_*10)

    //    val list=List(5,1,2,4,3)
    //
    //    val list2= list.sorted.reverse
    //
    //    list2.foreach(println(_))


    //    val list=List("1 hadoop","2 spark","3 flink")
    //
    //    val list2= list.sortBy(x=>x.split(" ")(0))
    //
    //    list2.foreach(println(_))

    //    val list = List(2,3,1,6,4,5)
    //
    //    val list2= list.sortWith((x,y)=>x<y)
    //    list2.foreach(println(_))

    //    val a = List("张三" -> "男", "李四" -> "女", "王五" -> "男")
    //
    //    val ss = a.groupBy(_._2)
    //
    //    for ((k, v) <- ss) {
    //      println(k + "*********" + v)
    //    }


    val a = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val list2 = a.reduce(_ + _)
    println(list2)

    println(a.foldRight(11)(_+_))

  }


}
