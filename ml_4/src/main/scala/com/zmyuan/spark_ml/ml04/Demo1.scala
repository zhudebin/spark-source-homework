package com.zmyuan.spark_ml.ml04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zdb on 2016/7/9.
  */
object Demo1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LR demo").setMaster("local[4]")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val data = MLUtils.loadLibSVMFile(sc, "D:\\workspace\\spark-source-homework\\ml_4\\data\\sample_libsvm_data.txt")

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
