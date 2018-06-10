package scp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import java.io._

sealed trait LinkDirection
case object In extends LinkDirection
case object Out extends LinkDirection
case object Both extends LinkDirection

class Graph(vids: RDD[Int], connections: RDD[(Int, Int)]) {

  val sc: SparkContext = SparkContext.getOrCreate()

  val edges: RDD[Edge] = connections.map{ case (srcId, dstId) => Edge(Vertex(srcId), Vertex(dstId))}

  val outlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.src).map(x => (x._1, x._2.map(e => e.dst))).filter(x => x._2.nonEmpty).sortByKey()
  val inlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.dst).map(x => (x._1, x._2.map(e => e.src))).filter(x => x._2.nonEmpty).sortByKey()
  val links: RDD[(Vertex, Iterable[Vertex], Iterable[Vertex])] = outlinks.join(inlinks).map(x => (x._1, x._2._1.map(v => v), x._2._2.map(v => v)))

  val vertices: RDD[Vertex] = links.map(x => new Vertex(x._1.vid, x._2.toSeq, x._3.toSeq))
//  println(s"Action on vertices (count = ${vertices.count()})")

//  println(s"Summed outdgrees ${vertices.map(v => {v.outdgr}).reduce(_+_)}")
//  println(s"Edges counted in Graph.scala is ${outlinks.map(x=>x._2.size).reduce((l1, l2)=> l1 + l2)}")

//  println("Outlinks")
//  var str = outlinks.map(e => s"${e._1.vid} -> " + s"${e._2.map(ed => ed.dst.vid)}")
//  str.foreach(println)

//  println("Inlinks")
//  str = inlinks.map(e => s"${e._1.vid} -> " + s"${e._2.map(ed => ed.src.vid)}")
//  str.foreach(println)

  def saveAsTextFile(outputFile: String, linkType: LinkDirection): Unit = {
    val graph_repr = linkType match {
      case In => inlinks.map(v => {
        s"${v._1.vid} -> (${v._2.map(in => in.vid)})"
      })
      case Out => outlinks.map(v => {
        s"${v._1.vid} -> (${v._2.map(out => out.vid)})"
      })
      case Both => links.map(v => {
        s"${v._1.vid} -> (${v._2.map(out => out.vid)}) <- (${v._3.map(in => in.vid)})"
      })
    }
    FileUtils.deleteDirectory(new File(outputFile))
    graph_repr.repartition(1).saveAsTextFile(outputFile)
  }

  def triangleCount(): Int = {
    val vs = vertices.collect()
    val triplets: Array[(Vertex, Seq[(Vertex, Vertex)])] = vs.map(v => (v, v.outpairs()))
    triplets.foreach(x => {
      print("vid " + x._1.vid + ": ")
      x._2.foreach(xx => {
        print(s"(${xx._1.vid}, ${xx._2.vid}) ")
      })
      println()
    })

    var count: Int = 0
    triplets.foreach(x => {
      x._2.foreach(xx => {
        if (xx._1.is_connected(xx._2)) {
          count += 1
        } else {
          println(s"No edge between ${xx._1.vid} and ${xx._2.vid}")
        }
      })
    })

    count
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