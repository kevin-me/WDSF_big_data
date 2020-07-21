import org.apache.spark.{SparkConf, SparkContext}

/**
 * def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
 * 函数作用同mapPartitions，不过提供了两个参数，第一个参数为分区的索引。
 */

object mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)


    var rdd1 = sc.makeRDD(1 to 5,2)


    var rdd2 = rdd1.mapPartitionsWithIndex{
      (x,iter) => {
        var result = List[String]()
        var i = 0
        while(iter.hasNext){
          i += iter.next()
        }
        result.::(x + "|" + i).iterator

      }
    }
    //rdd2将rdd1中每个分区的数字累加，并在每个分区的累加结果前面加了分区索引

    val ss=rdd2.collect

    print(ss.toBuffer)
  }

}
