package com.zmyuan.spark_ml.ml03

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zdb on 2016/7/3.
  */
object No1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("线性回归").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val trainDataPath = "D:\\workspace\\spark-source-homework\\ml_3\\data\\sample_linear_regression_data.txt"

    val trainData = sc.textFile(trainDataPath).map(line => {
      var idx = 0
      val strs = line.split("\\s").map(str => {
        if(idx == 0) {
          idx += 1
          str.toDouble
        } else {
          str.split(":")(1).toDouble
        }
      })
      LabeledPoint(strs(0), Vectors.dense(strs.slice(1, strs.length)))
    })

    val num = trainData.count()

    // 新建线性回归模型， 并设置训练参数
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 0.3
    val model = LinearRegressionWithSGD.train(trainData, numIterations, stepSize, miniBatchFraction)

    println(model.weights)
    println(model.intercept)

    // 对样本进行测试
    val prediction = model.predict(trainData.map(_.features))
    val predictionAndLabel = prediction.zip(trainData.map(_.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction \t label")

    print_predict.foreach(pal => {
      println(s"${pal._1}\t${pal._2}")
    })

    // 计算测试误差
    val loss = predictionAndLabel.map{
      case (p, l) => {
        val err = p - l
        err * err
      }
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / num)
    println(s"Test RMSE = $rmse .")

    // 模型保存
    val modelPath = "D:\\workspace\\spark-source-homework\\ml_3\\data\\sample_linear_regression_data_model"
    model.save(sc, modelPath)

    // 模型加载
    val sameModel = LinearRegressionModel.load(sc, modelPath)
    println(sameModel.weights)
    println(sameModel.intercept)
  }

}
