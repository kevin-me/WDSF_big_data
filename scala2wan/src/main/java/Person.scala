class Person(var name: String, var age: Int) {

  override def toString = s"Person ($name,$age)"
}

object Person {


  def apply(name: String, age: Int): Person = new Person(name, age)

  def unapply(arg: Person): Option[(String, Int)] = Some((arg.name, arg.age))

  def apply(name: String): Person = new Person(name, 20)


}


class Person4
class Student4 extends Person4

object Main4 {
  def main(args: Array[String]): Unit = {
    val s1:Person4 = new Student4

    // 判断s1是否为Student4类型
    if(s1.isInstanceOf[Person4]) {
      // 将s1转换为Student3类型
      val s2 =  s1.asInstanceOf[Person4]
      println(s2)
    }

  }
}


class Person5
class Student5 extends Person5

object Student5{
  def main(args: Array[String]) {
    val p:Person5=new Student5
    //判断p是否为Person5类的实例
    println(p.isInstanceOf[Person5])//true

    //判断p的类型是否为Person5类
    println(p.getClass == classOf[Person5])//false

    //判断p的类型是否为Student5类
    println(p.getClass == classOf[Student5])//true
  }
}



class Person8(var name:String){
  println("name:"+name)
}

// 直接在父类的类名后面调用父类构造器
class Student8(name:String, var clazz:String) extends Person8(name)

object Main8 {
  def main(args: Array[String]): Unit = {
    val s1 = new Student8("张三", "三年二班")
    println(s"${s1.name} - ${s1.clazz}")
  }
}


abstract class Person9(val name:String) {
  //抽象方法
  def sayHello:String
  def sayBye:String
  //抽象字段
  val address:String
  val ccc:String
  def cccc:String

}
class Student9(name:String) extends Person9(name){
  //重写抽象方法
  def sayHello: String = "Hello,"+name
  def sayBye: String ="Bye,"+name
  def cccc:String ="sss"+name
  //重写抽象字段
  override val address:String ="beijing "

  override val ccc: String = "sss"
}
object Main9{
  def main(args: Array[String]) {
    val s = new Student9("tom")
    println(s.sayHello)
    println(s.sayBye)
    println(s.address)
    print(s.ccc)
  }
}