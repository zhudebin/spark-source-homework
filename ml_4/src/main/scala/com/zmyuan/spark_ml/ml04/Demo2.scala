package com.zmyuan.spark_ml.ml04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zdb on 2016/7/9.
  */
object Demo2 {

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
    val appId2idxMap = rdd.map(t3 => t3._3).distinct().collect().map(appId => {
      idx += 1
      (appId, idx)
    }).toMap

    println(appId2idxMap.size)
    println(s"----------${appId2idxMap}")

    val appId2idxMap_bc = sc.broadcast(appId2idxMap)

    val userId2features = rdd.map(t3 =>{
      (t3._2, t3._3)
    }).groupByKey(10).mapPartitions((t2) => {
      val map = appId2idxMap_bc.value

      t2.map((t2) => {

        val features:Array[Double] = Array.fill(map.size)(0.0)
        // 直接生成features

        t2._2.foreach(appId => {
           val idx = map.get(appId).getOrElse(-1)
          if(idx > -1) {
            features(idx) = 1.0d
          }
        })

        (t2._1, features)
      })

    })

    val userId2label = sc.textFile("D:\\workspace\\spark-source-homework\\ml_4\\data\\labels\\*").map(str => {
      val strs = str.split("\\s")
      // userId, date, label
      (strs(0), strs(1), strs(2))
    }).filter(t3 => t3._2 == "2016-03-29").map(t3 => (t3._1, t3._3.toDouble))

    val data =userId2features.join(userId2label).map((t2) => {
      val label = if(t2._2._2 > 0.5) 1.0d else 0.0d
      LabeledPoint(label, Vectors.dense(t2._2._1))
    })

    //val data = MLUtils.loadLibSVMFile(sc, "D:\\workspace\\spark-source-homework\\ml_4\\data\\sample_libsvm_data.txt")

    // 样本数据划分训练样本与测试样本
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // 新建逻辑回归模型，并训练
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 0.5
    val model = LogisticRegressionWithSGD.train(training, numIterations, stepSize, miniBatchFraction)
    println(model.weights)
    println(model.intercept)

    // 对测试样本进行测试
    val predictionAndLLabels = test.map{
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    val print_predict = predictionAndLLabels.take(20)
    println(s"prediction\tlabel")
    print_predict.foreach(t2 => {
      println(s"${t2._1}\t${t2._2}")
    })

    // 误差计算
    val metrics = new MulticlassMetrics(predictionAndLLabels)
    val precision = metrics.precision
    println(s"Precision = $precision")

    sc.stop()
  }

}
