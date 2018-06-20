import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//sealed trait ED
//case object EI extends ED
//case object EO extends ED
//case object EB extends ED
//
//case class V(vid: Int, in: Seq[V], out: Seq[V]) {
//
//  def links(linkType: ED): Set[V] = {
//    linkType match {
//      case EI => in.toSet
//      case EO => out.toSet
//      case EB => out.toSet ++ in.toSet
//    }
//  }
//
//  def add_neighbour(linkType: ED, v: V): V = {
//    if (v.vid == vid) {
//      print("ERROR: self-edge")
//      sys.exit(-1)
//    }
//    linkType match {
//      case EI =>
//        V(vid, in :+ V(v.vid, Seq(), Seq()), out)
//      case _ =>
//        V(vid, in, out :+ V(v.vid, Seq(), Seq()))
//    }
//  }
//
//  def outdegree: Int = out.size
//  def indegree: Int = in.size
//  def degree: Int = outdegree + indegree
//
//  def outpairs(linkType: ED): Seq[(V, V)] = {
//    links(linkType).toSeq.combinations(2).map{case Seq(x, y) => (x, y)}.toSeq
//  }
//
//  def is_connected(target: V, linkType: ED): Boolean = {
//    links(linkType).exists(v => v.vid == target.vid)
//  }
//
//  var tcount = 0
//
//  override def toString: String = {
//    s"${in.map(x => x.vid)}" + " -> " + vid + " -> " + s"${out.map(x => x.vid)}"
//  }
//}

object Test {
  def main(args: Array[String]): Unit = {
//    val a: V = V(1, Seq(), Seq())
//    val aa: V = a.add_neighbour(EO, V(2, Seq(), Seq()))
//    val b = V(3, Seq(), Seq())
//    val c: V = V(4, Seq(aa), Seq(b))

//    println(a)
//    println(b)
////    b.links(EB).foreach(println)
//    println(c)
//    c.links(EI).foreach(println)


    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val vs = sc.parallelize(Seq(a, b, c))
//    vs.map(x => x.links(EO).foreach(x => println(x.vid)))


    val (nodes, connections) = (sc.parallelize(Seq(0, 1, 2, 3)), sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))

    val edges: RDD[Edge] = connections.map{case (srcId, dstId) => Edge(Vertex(srcId, None, None), Vertex(dstId, None, None))}

    val outlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.src).map(x => (x._1, x._2.map(e => e.dst)))
    val inlinks: RDD[(Vertex, Iterable[Vertex])] = edges.groupBy(edge => edge.dst).map(x => (x._1, x._2.map(e => e.src)))

    val links: RDD[(Vertex, Option[Iterable[Vertex]], Option[Iterable[Vertex]])] = outlinks.fullOuterJoin(inlinks).map(x => (x._1, x._2._1.map(v => v), x._2._2.map(v => v))).sortBy(_._1.vid)

    val vertices: RDD[Vertex] = links.map(x => Vertex(x._1.vid, x._2, x._3)).sortBy(_.vid)



  }
}
