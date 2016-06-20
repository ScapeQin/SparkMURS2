package org.apache.spark.murs

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-6-3.
 */
object CCApp {

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

    var ranks = links.map(eMsg => (eMsg._1, eMsg._1))
    for (i <- 1 to iter) {
      val contribs = links.join(ranks).values.flatMap{ value =>
        value._1.map(vtx => (vtx, math.min(vtx, value._2)))
      }
      ranks = contribs.reduceByKey((v1,v2) => math.min(v1, v2))
    }

    ranks.saveAsTextFile(args(3))
  }

}
