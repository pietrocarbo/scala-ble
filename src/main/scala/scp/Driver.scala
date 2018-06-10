package scp

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.{GenMap, GenSeq}

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

object Driver {

  def createSparkContext(master: String, appName: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName(appName)
    new SparkContext(conf)
  }

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = createSparkContext(master = "local[*]", appName = "MySparkApp")

//  val links: RDD[(Int, Int)] = sc.parallelize( Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3) )  // test graph

//    val (nodes, edges) = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt").parse()
//    println(s"Parsed ${nodes.size} nodes and ${edges.size} edges")

//    val (nodes, edges) = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt").parseIntoRDDs()
//    require(nodes.count() == 75879 && edges.count() == 508837, "Error during parsing")
    //    val graph = new Graph(nodes, edges)


    val graph = new Graph(
      sc.parallelize(Seq(0, 1, 2, 3)),
      sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))
//  graph.saveAsTextFile("graph", scp.Both)

    println(s"Number of triangles is ${graph.triangleCount()}")



    //    while (true) { thread.sleep(3) } // to keepalive the web UI
    sc.stop()
  }

}
// TODO
//  Graph metrics:
//  - triangle count (1624481 for Epinions)
//  - dynamic pagerank
//  - single source shortest paths
//  - (weakly/strongly) connected components
//  - topological sort
//
//  Perfomance study on different contexts (bigger dataset and cluster deploy)


