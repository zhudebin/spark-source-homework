package com.zmyuan.sparkSource.hw05

import java.io._

import org.apache.spark._
import org.apache.spark.streaming._
/**
  * Created by zdb on 2016/5/22.
  */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 8818)

    val words = lines.map(line => {
      println("-----------------" + line)
      line.trim
    })flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.filter(t2 => {
      t2._2>1
    }).foreachRDD((rdd, time) => {
      val list = rdd.collect()
      if(list.size > 0) {
        val file = new File("wc_" + time.milliseconds + ".txt")
        val bw = new BufferedWriter(new FileWriter(file))
        list.foreach(t2 => {
          val word = t2._1
          val count = t2._2
          bw.write(s"$word,$count")
          bw.newLine()
        })
        bw.flush()
        bw.close()
      }
    })
//      .saveAsTextFiles("wc", "txt")

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
