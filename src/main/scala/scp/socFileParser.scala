package scp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class socFileParser(filename: String) {

  def parse(): (RDD[Map[Int, Int]], RDD[Int])  = {
    val sc: SparkContext = SparkContext.getOrCreate()

    val edges = sc.textFile(filename).filter(line => !line.startsWith("#")).map(line => line.split("\t")).map(nodes => Map(nodes(0).toInt -> nodes(1).toInt))
    println("Parsed ", edges.count(), " edges in ", filename)

    val nodesSet: Set[Int] = sc.textFile(filename).filter(line => !line.startsWith("#")).flatMap(line => line.split("\t")).distinct.map(node => node.toInt).collect().toSet
    val nodes = sc.parallelize(nodesSet.toSeq)
    println("Parsed ", nodesSet.size, " nodes in ", filename)

    (edges, nodes)
  }

}