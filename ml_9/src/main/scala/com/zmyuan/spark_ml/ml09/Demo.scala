package com.zmyuan.spark_ml.ml09

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zdb on 2016/8/5.
  */
object Demo {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("als").setMaster("local[4]")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
    val rdd = sc.textFile("D:\\workspace\\spark-source-homework\\ml_4\\data\\userData\\*").map(str => {
      val strs = str.split("\\s")
      // date, userId, appId
      (strs(0), strs(1), strs(2))
    }).filter(t3 => t3._1 == "2016-03-29")

    val top100 = rdd.map(t3 => (t3._3, 1)).reduceByKey(_ + _).top(100)(new Ordering[(String, Int)](){
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        x._2 - y._2
      }
    })

    val top100RDD = sc.parallelize(top100)

    val userdata = rdd.map(t3 => (t3._3, t3)).join(top100RDD).map(t2 => {
      ItemPref(t2._2._1._2, t2._1, t2._2._2.toDouble)
    })

//    userdata.take(100).map(println)

    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, 30)

    //3 打印结果
    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
    simil_rdd1.collect().foreach { ItemSimi =>
      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
    }
//    println(s"用戶推荐列表: ${recommd_rdd1.count()}")
//    recommd_rdd1.collect().foreach { UserRecomm =>
//      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
//    }

    sc.stop()
  }
}
