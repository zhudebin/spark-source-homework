package com.zmyuan.spark_ml.ml04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zdb on 2016/7/9.
  */
object Demo3 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LR demo2").setMaster("local[4]")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
    val rdd = sc.textFile("D:\\workspace\\spark-source-homework\\ml_4\\data\\userData\\*").map(str => {
      val strs = str.split("\\s")
      // date, userId, appId
      (strs(0), strs(1), strs(2))
    }).filter(t3 => t3._1 == "2016-03-29")

    // 量化所有的appId
    var idx = -1
    val appId2idxMap = rdd.map(t3 => t3._3).collect().map(appId => {
      idx += 1
      (appId, idx)
    }).toMap

    println(appId2idxMap)

    sc.stop()
  }

}
