class ClassObject {

  val id = 1
  private var name = "kaikeba"

  def printName(): Unit = {
    //在Dog类中可以访问伴生对象Dog的私有属性
    println(ClassObject.CONSTANT + name)
  }

}

object ClassObject {
  //伴生对象中的私有属性
  private val CONSTANT = "汪汪汪 : "

  def main(args: Array[String]) {
    val p = new ClassObject
    //访问私有的字段name
    p.name = "123"
    p.printName()
  }
}