package org.apache.spark.murs

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-6-3.
 */
object Cache {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(args(6)).set("spark,murs.samplingInterval", args(0))
      .set("spark.murs.yellow", args(4)).set("spark.murs.multiTasks", args(5))
    val sparkContext = new SparkContext(sparkConf)

    val firstRDD = sparkContext.textFile(args(1), args(2).toInt)
    val cacheRDD = firstRDD.map(line => {
      val parts = line.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val cacheNewRDD = cacheRDD.mapValues((_ * 2))

    val saveRDD = cacheRDD.union(cacheNewRDD)

    saveRDD.saveAsTextFile(args(3))
  }

}
