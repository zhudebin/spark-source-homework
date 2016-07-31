package com.zmyuan.spark_ml.ml07

import java.io.{BufferedWriter, File, FileOutputStream}

import breeze.io.TextWriter.FileWriter

import scala.util.Random

/**
  * Created by zdb on 2016/7/31.
  */
object InitData {

  def main(args: Array[String]) {
    val filePath = "D:\\workspace\\spark-source-homework\\ml_7\\docs\\kmeans_data.txt"

    val random = new Random()
    val tenPoint = Seq( // 每个点的距离都在5以上
      (1.0, 1.0),
      (10.0, 10.0),
      (10.0, 20.0),
      (20.0, 10.0),
      (20.0, 20.0),
      (30.0, 20.0),
      (30.0, 10.0),
      (30.0, 30.0),
      (20.0, 30.0),
      (10.0, 30.0))

    val fw = new FileWriter(new File(filePath))

    for(i <- 0 to 10000) {
      val idx = random.nextInt(10)
      val point = tenPoint(idx)
      val rp = (random.nextFloat() % 3 + point._1, random.nextFloat() % 3 + point._2)
      fw.append(s"${rp._1} ${rp._2}\n")
    }
    fw.close()

  }

}
