import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import java.io._

import org.apache.spark.storage.StorageLevel

class Graph(vids: RDD[Int], connections: RDD[(Int, Int)], options: (Boolean, Boolean)) {

  var conns: connections.type = connections.persist(StorageLevel.MEMORY_AND_DISK_SER)

  val edges: RDD[Edge] = connections.map{ case (srcId, dstId) => Edge(Vertex(srcId, None, None), Vertex(dstId, None, None))}

  val outlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.src).map(x => (x._1, x._2.map(e => e.dst)))
  val inlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.dst).map(x => (x._1, x._2.map(e => e.src)))

  val links: RDD[(Vertex, Option[Iterable[Vertex]], Option[Iterable[Vertex]])] = outlinks.fullOuterJoin(inlinks)
    .map(x => (x._1, x._2._1.map(v => v), x._2._2.map(v => v)))
    .sortBy(_._1.vid).persist()
  val vertices: RDD[Vertex] = links.map(x => Vertex(x._1.vid, x._2, x._3)).sortBy(_.vid).persist()

  def allOps = {
    trasform(EdgeBoth)
    dynamicPageRank(8)
    friendsRecommendations()
    triangleCount()
  }

  def saveDataAsTextFile(data: RDD[_], outputName: String, outputBase: String = "outputs"): Unit = {
    FileUtils.deleteDirectory(new File(outputBase + outputName))
    data.saveAsTextFile(outputBase + outputName)
  }

  def trasform(linkType: EdgeDirection, outputFolder: String = "trasformed"): Unit = {

    val (outputs, secs) = Timer.time({
      val graph_repr = linkType match {
        case EdgeIn =>
          conns.groupBy(x => x._2)
            .map(x => (x._1, x._2.map(x => x._1)))
            .map(x => {
              s"${x._1} -> (${x._2.map(identity)})"})

        case EdgeOut =>
          conns.groupByKey()
            .map(v => {
              s"${v._1} -> (${v._2.map(identity)})"})

        case EdgeBoth =>
          conns.groupBy(x => x._2)
            .map(x => (x._1, x._2.map(x => x._1)))
            .join(conns.groupByKey())
            .map(v => {
              s"${v._2._1.map(identity)} -> " + s"${v._1} -> " + s"${v._2._2.map(identity)}"})
      }
      (graph_repr, graph_repr.count())
    })

    println(s"trasform() op DONE: result has ${outputs._2} elements (elapsed $secs seconds)")
    if (options._2) saveDataAsTextFile(outputs._1, outputFolder)
  }

  def dynamicPageRank(tollDigits: Int, outputFolder: String = "dynamicPageRank"): Unit = {
    val errorToll: Double = 1.0f / Math.pow(10, tollDigits)

    val (outputs, secs) = Timer.time({
      val links = conns.groupByKey()
      var ranks = links.mapValues(v => 1.0)
      var prevItRanks = links.mapValues(v => 1.0)

      while (ranks.join(prevItRanks).map{ case (v, (r, pr)) => r - pr <  errorToll}.reduce(_&&_)) {
        prevItRanks = ranks.map(x => (x._1, x._2))

        val contributions = links.join(ranks)
          .flatMap {
            case (u, (uLinks, rank)) =>
              uLinks.map(t => (t, rank / uLinks.size))
          }
        ranks = contributions.reduceByKey((x,y) => x + y)
          .mapValues(v => 0.15 + 0.85 * v)
      }
      (ranks, ranks.count())
    })

    println(s"dynamicPageRank() op DONE: result has ${outputs._2} elements (elapsed $secs seconds)")
    if (options._2) saveDataAsTextFile(outputs._1.sortBy(_._2, ascending = false), outputFolder)
  }

  def friendsRecommendations(outputFolder: String = "friendsRecommendations"): Unit = {

  }

  def triangleCount(outputFolder: String = "triangles"): Unit = {

      val (outputs, secs) = Timer.time({
        val triangles = conns.map(x => (x._2, x._1))
          .join(conns)
          .flatMap(x => Seq((x._2._1, x), (x._2._2, x)))
          .join(conns)
          .filter(x => (x._1 == x._2._1._2._1 && x._2._1._2._2 == x._2._2)
            ||
            (x._1 == x._2._1._2._2 && x._2._1._2._1 == x._2._2)
          ).map(x => {
            val c = if (x._1 == x._2._1._2._1) 1.0f else 1.0f / 3
            (Set(x._2._1._1, x._2._1._2._1, x._2._1._2._2), c)
        }).reduceByKey(_ + _)

        (triangles, triangles.count())
      })

    println(s"triangleCount() op DONE: result has ${outputs._2} elements (elapsed $secs seconds)")
    if (options._2) saveDataAsTextFile(outputs._1, outputFolder)
  }
}

//    Triangle count by GraphX
//    val graph = GraphLoader.edgeListFile(sc, "/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt",
//      true)
//      .partitionBy(PartitionStrategy.RandomVertexCut)
//    println(s"${graph.vertices.count()}, ${graph.edges.count()}")
//    val triCounts = graph.triangleCount().vertices
//    println(s"Triangle count by GraphX is ${triCounts.map(x=>x._2).sum()/3}")  // correct is 1624481

//    v1 - Triangle counting by parallel enumerating triples (O(n^3))
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

//    v2 - Triangle counting by sequential triplets enumeration
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

//    v3 - Triangle counting by auxiliary non-RDD map access
//    val vmap: collection.Map[Int, Vertex] = vertices.map(v => (v.vid, v)).collectAsMap()
//    val triplets: RDD[(Vertex, Seq[(Vertex, Vertex)])] = vertices.map(v => (v, v.outpairs(linkType)))
//
//    val tcount: Double = triplets.zipWithIndex.map(x => {
////      if (x._2 % 100 == 0) println(s"count at vertex n. ${x._2}\n")
//      x._1._2.map(xx => {
//        if (vmap(xx._1.vid).is_connected(xx._2, linkType)) {
//          1
//        }
//        else {
////          println(s"No edge between ${xx._1.vid} and ${xx._2.vid}")
//          0
//        }
//      }).sum
//    }).sum
//
//    if (linkType == EdgeBoth) {
//      require(tcount >= 3 && tcount % 3 == 0, "Error counting triangles")  // 4873443 for Epinions
//      (tcount / 3).toInt
//    } else {
//      tcount.toInt
//    }