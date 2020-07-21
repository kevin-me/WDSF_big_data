import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkBroadCast {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("broadcast").setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)

    val productRdd: RDD[String] = sparkContext.textFile("file:///C:\\Users\\Administrator\\Desktop\\review\\spark\\spark_day02\\2、数据\\广播变量实战\\pdts.txt")

    val mapProduct: collection.Map[String, String] = productRdd.map(x => {
      (x.split(",")(0), x)
    }).collectAsMap()

    def func[T](index: Int, iter: Iterator[(T)]): Iterator[String] = {
      iter.map(x => "[partID:" + index + " value:" + x + "]")
    }

    println(productRdd.mapPartitionsWithIndex(func).collect.toList)

    println("sssss"+mapProduct.toBuffer+mapProduct("p0003"))

    val broadCastValue: Broadcast[collection.Map[String, String]] = sparkContext.broadcast(mapProduct)

    val ordersRDD: RDD[String] = sparkContext.textFile("file:///C:\\Users\\Administrator\\Desktop\\review\\spark\\spark_day02\\2、数据\\广播变量实战\\orders.txt")
    //订单数据rdd进行拼接商品数据
    val proudctAndOrderRdd: RDD[String] = ordersRDD.mapPartitions(eachPartition => {
      val getBroadCastMap: collection.Map[String, String] = broadCastValue.value
      val finalStr: Iterator[String] = eachPartition.map(eachLine => {
        val ordersGet: Array[String] = eachLine.split(",")
        val getProductStr: String = getBroadCastMap.getOrElse(ordersGet(2), "")
        eachLine + "\t" + getProductStr
      })
      finalStr
    })
    println("****"+proudctAndOrderRdd.collect().toBuffer)
    sparkContext.stop()
  }


}
