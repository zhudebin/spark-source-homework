package com.zmyuan.ml.l02

import breeze.linalg._
import breeze.numerics._

/**
  * Created by zdb on 2016/6/26.
  */
object E1 {

  def main(args: Array[String]) {

    val dm1 = DenseMatrix.zeros[Double](2, 2)
    pl(dm1)

    val dv1 = DenseVector.zeros[Double](2)
    pl(dv1)

    val dv2 = DenseVector.ones[Double](3)
    pl(dv2)

    val dv3 = DenseVector.fill(3)(5.0)
    pl(dv3)

    val dv4 = DenseVector.range(10, 20, 3)
    pl(dv4)

    // 线性等分向量
    val dv5 = linspace(0, 20, 15)
    pl(dv5)

    // 单位矩阵
    val dm2 = DenseMatrix.eye[Double](3)
    pl(dm2)

    // 对角矩阵
    val dm3 = diag(DenseVector(1.0, 2.0, 3.0))
    pl(dm3)

    // 按照行创建矩阵
    val dm4 = DenseMatrix((1.0,2.0),(3.0, 4.0))
    pl(dm4)

    // 按照行创建向量
    val dv6 = DenseVector(1, 2, 3, 4)
    pl(dv6)

    // 向量转置
    val dv7 = DenseVector(1, 2, 3, 4).t
    pl(dv7)

    // 从函数创建向量
    val dv8 = DenseVector.tabulate(3){i => i*2}
    pl(dv8)

    // 从函数创建矩阵
    val dm5 = DenseMatrix.tabulate(3, 2){case(i,j) => i+j}
    pl(dm5)

    // 从数组创建向量
    val dv9 = new DenseVector(Array(1, 2, 3, 4))
    pl(dv9)

    // 从数组创建矩阵
    val dm6 = new DenseMatrix(2, 3, Array(11, 12, 13, 21, 22, 23))
    pl(dm6)

    // 从0到1的随机向量
    val dv10 = DenseVector.rand(4)
    pl(dv10)

    // 从0到1的随机矩阵
    val dm7 = DenseMatrix.rand(2, 3)
    pl(dm7)

    println(dm7(1,2))
  }

  def pl(any:Any): Unit = {
    println("-------------------")
    println(any)
  }

}
