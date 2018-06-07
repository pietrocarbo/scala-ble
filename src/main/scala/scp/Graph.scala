package scp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import java.io._

class Graph(vids: RDD[Int], links: RDD[(Int, Int)]) {

  val sc: SparkContext = SparkContext.getOrCreate()

  val vertices: RDD[Vertex] = vids.map(x => Vertex(x))
  val edges: RDD[Edge] = links.map{ case (srcId, dstId) => Edge(Vertex(srcId), Vertex(dstId))}

  edges.groupBy(edge => edge.src).map{ case (srcV, dstVs) => srcV.set_out_edges(dstVs.toSeq)}
  edges.groupBy(edge => edge.dst).filter(x => x._2.nonEmpty).map{ case (dstV, srcVs) => dstV.set_in_edges(srcVs.toSeq) }

  def saveToFile(outputFile: String): Unit = {
    val graph_repr = edges.groupBy(edge => edge.src).map(e => {
      s"${e._1.vid} -> " + s"${e._2.map(ed => ed.dst.vid)}"
    })
    FileUtils.deleteDirectory(new File(outputFile))
    graph_repr.repartition(1).saveAsTextFile(outputFile)
  }

  def triangleCount(): Int = {
    -1
  }
}

//    Triangle count by GraphX
//    val graph = GraphLoader.edgeListFile(sc, "/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt",
//      true)
//      .partitionBy(PartitionStrategy.RandomVertexCut)
//    println(s"${graph.vertices.count()}, ${graph.edges.count()}")
//    val triCounts = graph.triangleCount().vertices
//    println(s"Triangle count by GraphX is ${triCounts.map(x=>x._2).sum()/3}")  // correct is 1624481

//    Triangle count by parallel enumerating triples (O(n^3))
//    val (nodes, edges) = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt").parseIntoRDDs()
//    val edgesFraction = edges.count()/10  // edges.count()=508837
//    val links = edges.zipWithIndex().filter(x=>x._2<edgesFraction-1)
//      .map(x=> x._1).repartition(8).persist()
//    val triCount = links.cartesian(links).cartesian(links)
//      .filter{
//        case (((x, y), (xx, yy)), (xxx, yyy)) =>
//          if
//          ( (y == xx && yy == xxx && yyy == x)   // I - II - III
//            ||
//            (y == xxx && yyy == xx && yy == x)   // I - III - II
//            ||
//            (yy == x && y == xxx && yyy == xx)   // II - I - III
//            ||
//            (yy == xxx && yyy == x && y == xx)   // II - III - I
//            ||
//            (yyy == x && y == xx && yy == xxx)   // III - I - II
//            ||
//            (yyy == xx && yy == x && y == xxx)   // III - II - I
//            ||
//            (x == xx && ((y == xxx && yy == yyy) || (y == yyy && yy == xxx)))  // I, II biforcano, III chiude da I->II o II->I
//            ||
//            (x == xxx && ((y == xx && yy == yyy) || (y == yy && yyy == xx)))  // I, III biforcano, II chiude da I->III o III->I
//            ||
//            (xx == xxx && ((y == yy && yyy == x) || (y == yyy && yy == x)))  // II, II biforcano, I chiude da II->III o III->II
//          ) {
////            println("Triangle passing through: ", (((x, y), (xx, yy)), (xxx, yyy)))
//            true
//          }
//          else {
////            println("No triangle passing through: ", (((x, y), (xx, yy)), (xxx, yyy)))
//            false
//          }
//      }
//      .count()
//    println(s"Considering ${links.count()} edges, Triangles count is $triCount")

//    Triangle counting by sequential triples enumeration
//    val (nodes, edges) = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt").parseIntoRDDs()
//    val links = edges.groupByKey()
//    println(trianglesCount(links.collect())) // 10^17 for Epinions!!
//    def trianglesCount(links: Array[(Int, Iterable[Int])]): Set[(Int, Int, Int)] = {  // too complex: O(#node * (mean edges list length)^2) = 54h
//      var triangles: Set[(Int, Int, Int)] = Set( (-1, -1, -1) )
//      var currentIter: Int = -1
//      val totalIter: Int = links.length
//      for (((k, vs), iter) <- links.zipWithIndex; // foreach node
//           vv <- vs;         // foreach node's neighbour
//           (kk, vvs) <- links.filter(x => x._1 == vv);   // search it's neighbour
//           vvv <- vvs if vvv != k;  // foreach of them
//           (kkk, vvvs) <- links.filter(x => x._1 == vvv);  // search it's neighbour (last vertex of triangle)
//           vvvv <- vvvs if vvvv == k ) { // if it is the start vertex, store the triangle
//        if (iter > currentIter) {
//          currentIter = iter
//          println(s"iter $currentIter/$totalIter")
//        }
//        triangles += ((k, vv, vvv))
//      }
//      triangles -= ((-1, -1, -1))
//      triangles
//    }