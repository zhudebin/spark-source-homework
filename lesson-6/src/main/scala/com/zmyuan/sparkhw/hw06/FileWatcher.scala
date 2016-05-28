package com.zmyuan.sparkhw.hw06

import java.nio.file._
import java.util.concurrent.TimeUnit

/**
  * Created by zhudebin on 16/5/28.
  */
object FileWatcher {

  def main(args: Array[String]) {

    val path = Paths.get("/Users/zhudebin/Documents/iworkspace/opensource/spark-source-homework/lesson-6/docs")
    val watchService = FileSystems.getDefault().newWatchService()
    path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
      StandardWatchEventKinds.ENTRY_CREATE,
      StandardWatchEventKinds.ENTRY_DELETE,
      StandardWatchEventKinds.OVERFLOW)

    import scala.collection.JavaConversions._

    while(true) {
      val wk = watchService.poll()
      if(wk != null) {
        wk.pollEvents().foreach(we => {
          println(s"----${we.kind()} \t ${we.count()} \t ${we.context()}")
        })
      } else {
//        println(s" no event")
      }
    }

  }

}
