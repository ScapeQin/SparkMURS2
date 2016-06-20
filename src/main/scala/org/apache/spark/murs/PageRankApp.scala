package org.apache.spark.murs

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-6-3.
 */
object PageRankApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(args(7)).set("spark,murs.samplingInterval", args(0))
      .set("spark.murs.yellow", args(4)).set("spark.murs.multiTasks", args(5))
    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile(args(1), args(2).toInt)
    val iter = args(6).toInt
    val links = lines.map( line => {
      val parts = line.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }).groupByKey().persist(StorageLevel.MEMORY_AND_DISK)

    var ranks = links.mapValues(v => 1.0)
    for (i <- 1 to iter) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.saveAsTextFile(args(3))
  }

}
