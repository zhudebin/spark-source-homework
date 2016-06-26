package com.zmyuan.sparkhw.hw09

/**
  * Created by zdb on 2016/6/19.
  */
class SqlParser(sql: String) {

  def parse(): Unit = {

    val wheres = sql.split("\\swhere\\s")


    val beforeWhere = """select (* | [a-z]) from [a-z]{1, n} (as [a-z]{1,n})""".r

    val beforeWhere(num, str) = wheres(0)

    println(num + ":" + str)

    if(wheres.length > 1) {
      parseWhere(wheres(1))
    }

  }

  def parseWhere(whereStr : String): Unit = {

  }

}

object SqlParser {
  def main(args: Array[String]) {
    new SqlParser("select * from table")
  }
}