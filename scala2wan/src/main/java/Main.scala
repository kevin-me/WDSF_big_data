object Main {

  def main(args: Array[String]): Unit = {

    val customer = new Custom

    customer.age = 10

    customer.name = "xiaoran"

    println(s"姓名: ${customer.name}, 性别：${customer.name}")

    customer.sayHello("sss")

  }
}
