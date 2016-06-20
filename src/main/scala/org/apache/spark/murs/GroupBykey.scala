package org.apache.spark.murs

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-6-3.
 */
object GroupBykey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(args(6)).set("spark,murs.samplingInterval", args(0))
      .set("spark.murs.yellow", args(4)).set("spark.murs.multiTasks", args(5))
    val sparkContext = new SparkContext(sparkConf)

    val firstRDD = sparkContext.textFile(args(1), args(2).toInt)
    val groupRDD = firstRDD.map(line => {
      val parts = line.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }).groupByKey()

    groupRDD.map( line => {
      (line._1, line._2.toArray.mkString(","))
    }).saveAsTextFile(args(3))
  }

}
