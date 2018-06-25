import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import java.io._

class Graph(vids: RDD[Int], connections: RDD[(Int, Int)]) {

  val edges: RDD[Edge] = connections.map{ case (srcId, dstId) => Edge(Vertex(srcId, None, None), Vertex(dstId, None, None))}

  val outlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.src).map(x => (x._1, x._2.map(e => e.dst)))
  val inlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.dst).map(x => (x._1, x._2.map(e => e.src)))

  val links: RDD[(Vertex, Option[Iterable[Vertex]], Option[Iterable[Vertex]])] = outlinks.fullOuterJoin(inlinks)
    .map(x => (x._1, x._2._1.map(v => v), x._2._2.map(v => v)))
    .sortBy(_._1.vid).persist()
  val vertices: RDD[Vertex] = links.map(x => Vertex(x._1.vid, x._2, x._3)).sortBy(_.vid).persist()


  def saveAsTextFile(linkType: EdgeDirection, outputFile: String = "graph"): Unit = {
    val graph_repr = linkType match {
      case EdgeIn => inlinks.map(v => {
        s"${v._1.vid} -> (${v._2.map(in => in.vid)})"
      })
      case EdgeOut => outlinks.map(v => {
        s"${v._1.vid} -> (${v._2.map(out => out.vid)})"
      })
      case EdgeBoth => links.map(v => {
        s"${v._3.map(vseq => vseq.toSeq.map(inv => inv.vid))} -> ${v._1.vid} -> ${v._2.map(vseq => vseq.toSeq.map(outv => outv.vid))}"
      })
    }
    FileUtils.deleteDirectory(new File(outputFile))
    graph_repr.repartition(1).saveAsTextFile(outputFile)
  }

  def dynamicPageRank(errorRate: Double = 1e-6, outputFile: String = "pageRank"): Unit = {

    val links = connections.groupByKey().persist()

    var ranks = links.mapValues(v => 1.0)
    var prevRanks = links.mapValues(v => 1.0)
    var errRate = 1e-8

    while (ranks.join(prevRanks).map{ case (v, (r, pr)) => r - pr <  errRate}.reduce(_&&_)) {
      prevRanks = ranks.map(x => (x._1, x._2))

      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, rank)) =>
          uLinks.map(t => (t, rank / uLinks.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).
        mapValues(v => 0.15 + 0.85*v)
    }

    FileUtils.deleteDirectory(new File(outputFile))
    ranks.sortBy(_._2, ascending = false).repartition(1).saveAsTextFile(outputFile)
  }

  def triangleCount(linkType: EdgeDirection = EdgeBoth): Int = {
    val vmap: collection.Map[Int, Vertex] = vertices.map(v => (v.vid, v)).collectAsMap()
    val triplets: RDD[(Vertex, Seq[(Vertex, Vertex)])] = vertices.map(v => (v, v.outpairs(linkType)))

    val tcount: Double = triplets.zipWithIndex.map(x => {
//      if (x._2 % 100 == 0) println(s"count at vertex n. ${x._2}\n")
      x._1._2.map(xx => {
        if (vmap(xx._1.vid).is_connected(xx._2, linkType)) {
          1
        }
        else {
//          println(s"No edge between ${xx._1.vid} and ${xx._2.vid}")
          0
        }
      }).sum
    }).sum

    if (linkType == EdgeBoth) {
      require(tcount >= 3 && tcount % 3 == 0, "Error counting triangles")  // 4873443 for Epinions
      (tcount / 3).toInt
    } else {
      tcount.toInt
    }
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

//    Triangle counting by sequential triplets enumeration
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