import java.util.Date
import java.text.SimpleDateFormat


object DateUtils {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  println("构造代码")

  def format(date: Date) = simpleDateFormat.format(date)

  def main(args: Array[String]): Unit = {

    println {
      DateUtils.format(new Date())
    };
  }

}
