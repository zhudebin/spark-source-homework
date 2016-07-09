package com.zmyuan.sparkhw.hw12

import java.io._

import breeze.io.TextWriter.FileWriter
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by zdb on 2016/7/9.
  */
object KmsDemo {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("kms").setMaster("local[4]")

    val sc = new SparkContext(conf)

//    val data = sc.parallelize(1 until 1000, 4).mapPartitions((iter) => {
//      val random = new Random()
//      iter.map(i => {
//        (i, random.nextInt(4000).toFloat, random.nextInt(8000).toFloat)
//      })
//    })

    val data = prepareData("D:\\workspace\\spark-source-homework\\lesson-12\\data\\trainData.txt", sc)

    val parsedData = data.map(t3 => {
      Vectors.dense(t3._2, t3._3)
    })

    //对数据集聚类，3个类，20次迭代，形成数据模型 注意这里会使用设置的partition数20
    val numClusters = 3
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)

    val modelBc = sc.broadcast(model)
    data.mapPartitions((iter) => {
      val m = modelBc.value

      iter.map(t3 => {
        val vector = Vectors.dense(t3._2, t3._3)
        (t3._1,t3._2, t3._3, m.predict(vector))
      })

    }).collect().foreach(println _)

    println("-------------------")
  }

  def prepareData(path:String, sc: SparkContext): RDD[(Int, Float, Float)] = {

    val file = new File(path)

    if(!file.exists()) {
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
      val random = new Random()
      (1 until 1000).map(i => {
        (i, random.nextInt(4000).toFloat, random.nextInt(8000).toFloat)
      }).map(t3 => {
        bw.write(s"${t3._1},${t3._2},${t3._3}\r\n")
      })
      bw.flush()
      bw.close()
    }

    sc.textFile(path, 4).map(line => {
      val strs = line.split(",")
      (strs(0).toInt, strs(1).toFloat, strs(2).toFloat)
    })
  }

}
