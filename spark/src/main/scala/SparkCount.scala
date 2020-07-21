import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkCount")
    val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)
    sparkContext.setLogLevel("WARN")
    val wholeWords: RDD[String] = sparkContext.textFile("hdfs://node01:8020/spark_count")
    val collectResult: Array[(String, Int)] = wholeWords.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y).collect()
    println(collectResult.toBuffer)

  }
}
