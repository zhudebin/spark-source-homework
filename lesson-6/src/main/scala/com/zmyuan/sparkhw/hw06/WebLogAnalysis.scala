package com.zmyuan.sparkhw.hw06

import java.io._

import org.apache.commons.csv.{CSVParser, CSVFormat}
import org.apache.spark._
import org.apache.spark.streaming._

/**
  * Created by zdb on 2016/5/22.
  */
object WebLogAnalysis {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WebLogAnalysis")

    val checkpointPath = "/Users/zhudebin/Documents/iworkspace/opensource/spark-source-homework/lesson-6/docs/streamck/"

    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      val ssc = new StreamingContext(conf, Seconds(3))
      ssc.checkpoint(checkpointPath)
      ssc
    })

    val lines = ssc.socketTextStream("localhost", 8818)


    val lines2 = lines.filter(!_.isEmpty).map(line => {
      val strs = line.split(" ")
      (strs(0), strs(6), strs(12))
    })

    lines2.map(t3 => (t3._1, 1)).reduceByKey(_ + _).updateStateByKey((values:Seq[Int], state:Option[Int]) => {
      var newSum = state.getOrElse(0)
      values.foreach(i => {
        newSum += i
      })
      Some(newSum)
    }).foreachRDD((rdd, time) => {
      val list = rdd.top(2)(new Ordering[(String, Int)] {
        override def compare(x: (String, Int), y: (String, Int)): Int = {
          val dif = x._2 - y._2
          if(dif > 0) {
            1
          } else if(dif == 0) {
            0
          } else {
            -1
          }
        }
      })
      println(s"------rdd id: ${rdd.id} -----")
      list.foreach(t2 => {
        println(s"${t2._1} cnt:${t2._2}, time:${time}")
      })
    })
//      .saveAsTextFiles("wc", "txt")

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
