package com.zmyuan.spark_ml.ml07

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zdb on 2016/7/31.
  */
object Demo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("kmeans").setMaster("local[4]")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本
    val data = sc.textFile("D:\\workspace\\spark-source-homework\\ml_7\\docs\\kmeans_data.txt")
    val parsedData = data.map(line => Vectors.dense(line.split("\\s").map(_.toDouble))).cache()

    // 新建kmeans模型
    val initMode = "k-means||"
    val k = 10
    val numIteration = 100
    val model = KMeans.train(parsedData, k, numIteration, 100, initMode)

    val centers = model.clusterCenters
    println("centers:\n")
    for(i <- 0 until centers.length) {
      println(s"${centers(i)(0)}\t${centers(i)(1)}")
    }

    // 误差计算
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)


  }

}
