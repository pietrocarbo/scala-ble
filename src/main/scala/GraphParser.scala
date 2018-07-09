import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


// A simple class with methods to parse 'soc' graph file format that is:
// # comment lines start with hashtag
// FromID\tToID
class GraphParser(filename: String) {

  def parseIntoRDDs(nPartitions: Int): RDD[(Int, Int)]  = {

    val sc: SparkContext = SparkContext.getOrCreate()

    val textFileRDD =
      if (nPartitions == 0) sc.textFile(filename)
      else sc.textFile(filename, nPartitions)

    textFileRDD.filter(line => !line.startsWith("#"))
      .map(line => line.split("\t"))
      .map(nodes => nodes(0).toInt -> nodes(1).toInt)
  }

  // Not used
  def parse(): (Set[Int], Seq[(Int, Int)]) = {

    val nodes: Set[Int] = Source.fromFile(filename).getLines().filter(line => !line.startsWith("#"))
      .flatMap(line => line.split("\t")).map(vid => vid.toInt).toSet

    val edges: Seq[(Int, Int)] = Source.fromFile(filename).getLines().filter(line => !line.startsWith("#"))
      .map(line => line.split("\t")).map(vids => (vids(0).toInt, vids(1).toInt)).toSeq

    (nodes, edges)
  }

}