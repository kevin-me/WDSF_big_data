import org.apache.spark.{SparkConf, SparkContext}


/**
 * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。 它的函数定义为：
 * def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
 * f即为输入函数，它处理每个分区里面的内容。每个分区中的内容将以Iterator[T]传递给输入函数f，f的输出结果是Iterator[U]。最终的RDD由所有分区经过输入函数处理后的结果合并起来的。
 *
 *
 */
object mapPartitionssz {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val a = sc.parallelize(1 to 9, 3)

    def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
      var res = List[(T, T)]()
      var pre = iter.next
      while (iter.hasNext) {
        val cur = iter.next
        res.::=(pre, cur)
        pre = cur
      }
      res.iterator
    }

    val ss = a.mapPartitions(myfunc).collect

    print(ss.toBuffer)

  }

}
