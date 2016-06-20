package org.apache.spark.murs

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-6-18.
 */
object SortApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(args(7)).set("spark,murs.samplingInterval", args(0))
      .set("spark.murs.yellow", args(4)).set("spark.murs.multiTasks", args(5))
    val sparkContext = new SparkContext(sparkConf)

    val ordering = implicitly[Ordering[Int]]
    val lines = sparkContext.textFile(args(1), args(2).toInt)
    val iter = args(6).toInt
    val links = lines.flatMap( line => {
      val parts = line.split("\\s+")
      parts.map(_.toInt)
    }).distinct().map(value => (value, 1))

    links.sortByKey().saveAsTextFile(args(3))
  }

}
