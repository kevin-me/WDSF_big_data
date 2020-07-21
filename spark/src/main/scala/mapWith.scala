import org.apache.spark.{SparkConf, SparkContext}

object mapWith {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)

    val x = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

//    val ss = x.mapWith(a => a * 10)((b, a) => (b, a + 2)).collect
//
//    print(ss.toBuffer)
  }
}
