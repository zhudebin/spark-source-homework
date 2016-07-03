package com.zmyuan.ml.l02

import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import collection.mutable.ArrayBuffer
import util.Random
/**
  * Created by zdb on 2016/6/26.
  */
object No1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Week2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 创建随机的二维数组
    val rows = 5 // 样本点数量
    val cols = 6 // 前 5 列是特征变量，最后一列为 Label
    val rand = new Random()
    val data = Array.ofDim[Double](rows, cols)
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (j == cols - 1) {
          data(i)(j) = rand.nextDouble() // y 值在 0-1 范围内
        } else {
          data(i)(j) = rand.nextDouble() * 20 // x 值在 0-20 范围内
        }
      }
    }
    // 随机生成参数 w
    val w = new Array[Double](cols)
    for (i <- 0 until cols) {
      w(i) = rand.nextDouble()
    }
    /**
      * 第一题
      */
    // 特征变量标准化
    val dataTrans = data.toSeq.transpose // 需要对每一列标准化，先做转置处理
    val rddTrans = sc.parallelize(dataTrans, 1)
    val rddTransSt = rddTrans.map(x => standard(x))

    val dataSt = rddTransSt.collect.toSeq.transpose
    val rddSt = sc.parallelize(dataSt)
    // 计算预测值
    val rddTemp = rddSt.map(x => arrayMultiply(x, w, cols - 1))
    val rddYPred = rddTemp.map(x => 1 / (1 + scala.math.exp(-x)))
    // 计算 RMSE
    val rddYTrue = sc.parallelize(data).map(x => x(rows)) // y 实际值
    val rdd1 = rddYTrue.zip(rddYPred).map(x => (x._1 - x._2) *
      (x._1 - x._2))
    val rmse1 = scala.math.sqrt(rdd1.sum() / rows)
    val YPredArray = rddYPred.collect()
    println("RDD 计算的预测值")
    println(YPredArray.deep.mkString(","))
    println("RDD 计算的 RMSE")
    println(rmse1)
    /**
      * 第二题
      */
    // 创建矩阵
    val v = ArrayBuffer[Double]()
    for (i <- 0 until cols) {
      v ++= dataTrans(i)
    }
    val dataMatrix = new DenseMatrix[Double](rows, cols, v.toArray)
    // 特征变量 X 标准化处理
    val X = dataMatrix(::, 0 until cols - 1)
    val XMean = mean(X(::, *)).toDenseVector
    val XStd = stddev(X(::, *)).toDenseVector
    X(*, ::) -= XMean
    X(*, ::) /= XStd
    // 计算预测值
    val Xones = DenseMatrix.horzcat(DenseMatrix.ones[Double](rows,
      1), X)
    val wMatrix = DenseMatrix(w)
    val temp = (Xones * wMatrix.t)
    val Ypred = (1.0 / (exp(temp * -1.0) + 1.0)).toDenseVector
    // 计算 RMSE
    val Ytrue = dataMatrix(::, -1)
    val rmse2 = sqrt(mean(pow(Ypred - Ytrue, 2)))
    println("矩阵计算的预测值")
    println(Ypred)

    println("矩阵计算的 RMSE")
    println(rmse2)
  }

  /**
    * 向量乘法
    */
  def arrayMultiply(x:Seq[Double], w:Array[Double], xlength:Int):
  Double = {
    var res = w(0)
    for (i <- 0 until xlength) {
      res = res + x(i) * w(i+1)
    }
    return res
  }
  /**
    * 数组标准化
    */
  def standard(x: Seq[Double]): Array[Double] = {
    val s = new Array[Double](x.length)
    val avg = x.sum/x.length
    var variance = 0.0
    for (i <- 0 until x.length) {
      variance += math.pow(x(i) - avg,2)
    }
    val std = math.sqrt(variance/(x.length-1.0)) // 样本标准偏差
    for (i <- 0 until x.length) {
      s(i) = (x(i)-avg)/std
    }
    return s
  }
}
