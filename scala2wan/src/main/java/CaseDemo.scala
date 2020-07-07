import scala.util.Random

object CaseDemo extends App {

  val arr = Array("hadoop", "zookeeper", "spark", "storm")

  val name = arr(Random.nextInt(arr.length))

  print(name)

  name match {
    case "hadoop" => print("我不认识hadoop")
    case "zookeeper" => print("我不认识zookeeper")
    case _ => print("我不认识你")

  }

}


//todo:匹配类型
object CaseDemo02 extends App {
  //定义一个数组
  val arr = Array(  CaseDemo02, 'c')

  //随机获取数组中的元素
  val value = arr(Random.nextInt(arr.length))
  println(value)


  value match {
    case x: Int => println("Int=>" + x)
    case y: Double if (y >= 0) => println("Double=>" + y)
    case z: String => println("String=>" + z)
    case f: Char => print("ssss"+f)
    case _ => throw new Exception("not match exception")
  }

}

//匹配数组
object CaseDemo03 extends App{

  //匹配数组
  val  arr=Array(1,3,5,6)
  arr match{
    case Array(1,x,y) =>println(x+"---"+y)
    case Array(1,_*)  =>println("1...")
    case Array(0)     =>println("only 0")
    case _            =>println("something else")

  }
}

//匹配集合
object CaseDemo04 extends App{

  val list=List(0,3,6)
  list match {
    case 0::Nil        => println("only 0")
    case 0::tail       => println("0....")
    case x::y::z::Nil  => println(s"x:$x y:$y z:$z")
    case _             => println("something else")
  }
}


//匹配元组
object CaseDemo05 extends App{

  val tuple=(1,3,5)
  tuple match{
    case (1,x,y)    => println(s"1,$x,$y")
    case (2,x,y)    => println(s"$x,$y")
    case _          => println("others...")
  }
}