import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object suanzi {

  def main(args: Array[String]): Unit = {

    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("jim", 2)))
    //分组
    val rdd3 = rdd1.cogroup(rdd2)
    val collect: Array[(String, (Iterable[Int], Iterable[Int]))] = rdd3.collect

   // print(collect.toBuffer)


    val words1 = Array("a", "a", "b", "b", "a", "a")

    val value: RDD[(String, Int)] = sc.parallelize(words1).map(x => (x, 1))

    val value1: RDD[(String, Iterable[Int])] = value.groupByKey()

    val value2: RDD[(String, Int)] = value1.map(x => (x._1, x._2.sum))

    val tuples: Array[(String, Int)] = value2.collect()

  //  print(tuples.toBuffer)

    val wordPairsRDD = sc.parallelize(words1).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD .reduceByKey(_ + _) .collect()

    val wordCountsWithGroup = wordPairsRDD .groupByKey() .map(t => (t._1, t._2.sum)) .collect()


    val rdd = sc.parallelize(List(("cat",2),("cat",5),("pig",10),("dog",3),("dog",4),("cat",4)),2)
    println("-------------------------------------华丽的分割线------------------------------------------")
    def func[T](index:Int,iter:Iterator[(T)]): Iterator[String] ={
      iter.map(x => "[partID:"+index+" value:"+x+"]")
    }
    println(rdd.mapPartitionsWithIndex(func).collect.toList)

    val rdd8: RDD[(String, Int)] = rdd.aggregateByKey(1)(math.max(_,_),_+_)
    println(rdd8.collect.toBuffer)

  }

}
