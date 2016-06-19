package com.zmyuan.sparkhw.hw07

import java.sql.DriverManager

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by zdb on 2016/5/22.
  */
object WebLogAnalysis {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WebLogAnalysis")

    val checkpointPath = "/Users/zhudebin/Documents/iworkspace/opensource/spark-source-homework/lesson-7/docs/streamck/"

    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      val ssc = new StreamingContext(conf, Seconds(3))
      ssc.checkpoint(checkpointPath)
      ssc
    })

    val lines = KafkaUtils.createStream(ssc, "192.168.0.90:2181", "1", Map("test" -> 1), StorageLevel.MEMORY_AND_DISK).map(t2 => {
      t2._2
    })


    val lines2 = lines.filter(!_.isEmpty).map(line => {
      val strs = line.split(" ")
      // IP, url, browers
      (strs(0), strs(6), strs(12))
    })

    def no1(): Unit = {
      // 第一题
      lines2.map(t3 => (t3._1, 1)).reduceByKey(_ + _).updateStateByKey((values:Seq[Int], state:Option[Int]) => {
        var newSum = state.getOrElse(0)
        values.foreach(i => {
          newSum += i
        })
        Some(newSum)
      }).foreachRDD((rdd, time) => {
        val list = rdd.top(10)(new Ordering[(String, Int)] {
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
        saveToDb("no1", list)
      })
    }

    def no2(): Unit ={
      // 第二题
      lines2.map(t3 => (ipToRegion(t3._1), 1)).reduceByKey(_ + _).updateStateByKey((values:Seq[Int], state:Option[Int]) => {
        var newSum = state.getOrElse(0)
        values.foreach(i => {
          newSum += i
        })
        Some(newSum)
      }).foreachRDD((rdd, time) => {
        // top
        val list = rdd.collect()
        println(s"------rdd id: ${rdd.id} -----")
        list.foreach(t2 => {
          println(s"${t2._1} cnt:${t2._2}, time:${time}")
        })
        saveToDb("no2", list)
      })
    }

    def no3(): Unit = {
      // 第三题
      lines2.map(t3 => (t3._2, 1)).window(Minutes(5)).reduceByKey(_ + _).foreachRDD(rdd => {
        val list = rdd.collect()
        saveToDb("no3", list)
      })

    }

    def ipToRegion(ip: String): String = {
      "zhangjiang"
    }

    def saveToDb(tableName:String, list: Seq[(String, Int)]): Unit = {
      if (list == null || list.size == 0) {
        return
      }
      Class.forName("com.mysql.jdbc.Driver")
      val con = DriverManager.getConnection("jdbc:mysql://192.168.137.90:3306/test")
      con.setAutoCommit(false)

      val pst = con.prepareStatement(s"insert into $tableName (c1, c2) values(?, ?)")
      for(t2 <- list) {
        pst.setString(1, t2._1)
        pst.setInt(2, t2._2)
        pst.addBatch()
      }

      pst.executeBatch()
      con.commit()
      con.close()
    }

//      .saveAsTextFiles("wc", "txt")

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
