package com.zmyuan.spark.ml.lesson01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhudebin on 16/6/15.
  */
object No1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ml1").setMaster("local[6]")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://192.168.137.101:8020/tmp/test/用户安装列表数据").map(str => {
      val strs = str.split("\\s")
      (strs(0), strs(1), strs(2))
    })

//    no1(rdd)
//    no2(rdd, "2016-04-09", "2016-04-10")
    no3(rdd, "2016-04-09")
  }

  def no1(rdd:RDD[(String,String, String)]): Unit = {
    val count = rdd.count()
    println(s"=====================================总数:$count")

    val userCount = rdd.map(t3 => t3._2).distinct().count()
    println(s"---------------------------------------用户总数:$userCount")

    val dates = rdd.map(t3 => t3._1).distinct().collect()
    dates.foreach{ date =>
      println(s"++++++++++++++++++++++++++++++++++++++++日期:$date")
    }
  }

  def no2(rdd:RDD[(String, String, String)], date1:String, date2:String): Unit = {
    val rdd1 = rdd.filter(t3 => t3._1.trim == date1).map(t3 => ((t3._2, t3._3), t3._1))
    val rdd2 = rdd.filter(t3 => t3._1.trim == date2).map(t3 => ((t3._2, t3._3), t3._1))

    rdd2.leftOuterJoin(rdd1).filter(t2 => {
      t2._2._2.isEmpty
    }).map(t2 => {
      // 用户ID, 应用名
      (t2._1._1, t2._1._2)
    }).reduceByKey(_ + "," + _)
      .collect()
      .foreach(t2 => {
        println(s"用户:${t2._1}, ${date2}新安装的应用:[${t2._2}]")
      })
  }

  /**
    *
    * @param rdd   上报日期、用户ID、安装包名
    * @param date 任意一天  yyyy-MM-dd
    */
  def no3(rdd:RDD[(String, String, String)], date:String): Unit = {

    def top100(): Array[String] = {
      val apps = rdd.filter(t3 => t3._1 == date).map(t3 => (t3._3, 1)).reduceByKey(_ + _).top(100)(new Ordering[(String, Int)]() {
        override def compare(x: (String, Int), y: (String, Int)): Int = {
          x._2 - y._2
        }
      }).map(t2 => {
        //      println(t2)
        t2._1
      })

      println("================== 安装量 top 100 ==================")
      apps.foreach(println)
      apps
    }

    val apps = top100()

    println()

    // 1. 先求总数
    val count = rdd.map(t3 => (t3._1, t3._2)).distinct().count()

    println(s"总数:$count")

    // 过滤 非前100的
    val bc_apps = rdd.context.broadcast(apps)

    val top100RDD = rdd.filter(t3 => {
      bc_apps.value.contains(t3._3)
    })

    // 求两两
    val pairRDD = top100RDD.map(t3 => {
      ((t3._1, t3._2), t3._3)
    })
    val rdd1 = pairRDD.join(pairRDD).filter(t2 => {
      t2._2._1 != t2._2._2
    })

    val one2oneRDD = rdd1.map(t2 => (sortedTuple2(t2._2._1, t2._2._2), 1)).reduceByKey(_ + _)
//    one2oneRDD.collect().foreach(t2 => println(t2._1 + " -> " + t2._2))

    // 求单一
    val oneRDD = top100RDD.map(t3 => (t3._3, 1)).reduceByKey(_ + _)
//    oneRDD.collect().foreach(t2 => println(s"one: ${t2._1} -> ${t2._2}"))

    // 求支持度  -> 过滤
    val stage1 = one2oneRDD.map(t2 => (t2._1, (t2._2,t2._2 / count.toFloat))).filter(t2 => {
      t2._2._2 >= 0.1
    })
    // -> 求 置信度 -> 过滤
    val resultRDD = stage1.map(t2 => {
      // app1, (app1, app2, app1&app2总数, 支持度)
      (t2._1._1, (t2._1._1, t2._1._2, t2._2._1, t2._2._2))
    }).join(oneRDD).map(t2 => {
      // app2, (app1, app2, app1&app2总数, 支持度, app1总数)
      (t2._2._1._2, (t2._2._1._1, t2._2._1._2, t2._2._1._3, t2._2._1._4, t2._2._2))
    }).join(oneRDD).map(t2 => {
      val t5 = t2._2._1
      // (app1, app2, app1&app2总数, 支持度, app1总数, app2总数)
      val (app1, app2, twoCount, support, app1Count, app2Count) = (t5._1, t5._2, t5._3, t5. _4, t5._5, t2._2._2)  // ((app1, app2, 支持度), app1单一, app2单一)  // todo 还需要两两的总数
      (app1, app2, support, twoCount/app1Count.toFloat, twoCount/app2Count.toFloat)
    }).filter(t5 => {
      t5._4 >= 0.3 && t5._5 >= 0.3
    })
    resultRDD.collect().foreach(t5 => println(s"========result====$t5"))
  }

  def sortedTuple2(str1:String, str2:String): (String, String) = {
    if(str1>=str2) {
      (str1, str2)
    } else {
      (str2, str1)
    }
  }
}

