import java.text.SimpleDateFormat
import java.util.Date

import javax.lang.model.element.NestingKind

class ConsoleLogger extends Logger1 with MessageSender {

  override def msg(msg: String): String = {
    msg
  }

  override def send(msg: String): Unit = {
    print(msg)
  }

  override def log(msg: String): Unit = {
    print(msg)
  }

  override val TYPE: String = "sss"
}


trait Logger1 {

  def msg(msg: String): String

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  val INFO = "信息:" + sdf.format(new Date)
  // 抽象字段
  val TYPE:String

  // 抽象方法
  def log(msg:String)

}

trait MessageSender {
  def send(msg: String)
}

object LoggerTrait2 {
  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLogger
   val sss= logger.msg("控制台日志: 这是一条Log")

    logger.send(sss)
    logger.send("你好!")
  }
}


trait LoggerMix {
  def log(msg:String) = println(msg)
}

class UserService

object FixedInClass {
  def main(args: Array[String]): Unit = {
    // 使用with关键字直接将特质混入到对象中
    val userService = new UserService with LoggerMix

    userService.log("你好")
  }
}