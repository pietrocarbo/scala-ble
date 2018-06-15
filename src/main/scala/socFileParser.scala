import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class socFileParser(filename: String) {

  def parseIntoRDDs(): (RDD[Int], RDD[(Int, Int)])  = {

    val sc: SparkContext = SparkContext.getOrCreate()

    val nodes: RDD[Int] = sc.textFile(filename).filter(line => !line.startsWith("#"))
      .flatMap(line => line.split("\t"))
      .map(node => node.toInt)
      .distinct()

    val edges = sc.textFile(filename).filter(line => !line.startsWith("#"))
      .map(line => line.split("\t"))
      .map(nodes => nodes(0).toInt -> nodes(1).toInt)

    (nodes, edges)
  }

  def parse(): (Set[Int], Seq[(Int, Int)]) = {

    val nodes: Set[Int] = Source.fromFile(filename).getLines().filter(line => !line.startsWith("#"))
      .flatMap(line => line.split("\t")).map(vid => vid.toInt).toSet

    val edges: Seq[(Int, Int)] = Source.fromFile(filename).getLines().filter(line => !line.startsWith("#"))
      .map(line => line.split("\t")).map(vids => (vids(0).toInt, vids(1).toInt)).toSeq

    (nodes, edges)
  }

}