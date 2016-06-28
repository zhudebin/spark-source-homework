package com.zmyuan.hw.l11


import org.apache.spark._
import org.apache.spark.graphx._

/**
  * Created by zhudebin on 16/6/28.
  */
object GraphX2 {

  def main(args: Array[String]) {
    // Connect to the Spark cluster
    val sc = new SparkContext("local[4]", "research")
//    val sc = new SparkContext("spark://master.amplab.org", "research")

    // Load my user data and parse into tuples of user id and attribute list
    val users = (sc.textFile("/Users/zhudebin/Documents/iworkspace/apache/spark-1.5.2/graphx/data/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))

    // Parse the edge data which is already in userId -> userId format
    val followerGraph = GraphLoader.edgeListFile(sc, "/Users/zhudebin/Documents/iworkspace/apache/spark-1.5.2/graphx/data/followers.txt")

    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }

}
