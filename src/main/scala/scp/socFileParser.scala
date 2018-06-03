package scp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class socFileParser(filename: String) {

  // (RDD[Map[Int, Int]], RDD[Set[Int]])
  def parse(): Unit  = {
    val sc: SparkContext = SparkContext.getOrCreate()

    val edges = sc.textFile(filename).filter(line => !line.startsWith("#")).map(line => line.split("\t")).map(nodes => Map(nodes(0).toInt -> nodes(1).toInt))
    println("Found ", edges.count(), " edges in ", filename)

    val nodes =
    println("Found ", nodes.size, " nodes in ", filename)
  }

}