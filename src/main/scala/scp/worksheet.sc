//"# Nodes: 75879 Edges: 508837".startsWith("#")
//
//import org.apache.spark.{SparkConf, SparkContext}
//val conf = new SparkConf()
//conf.setMaster("local[*]")
//conf.setAppName("Simple Test Application")
//val sc = new SparkContext(conf)

//var triples: Set[(Int, Int, Int)] = Set( (-1, -1, -1) ) // = Set( (1, 2, 4), (0, 4, 6) )
//triples += ((1, 2, 5))
//triples -= ((-1, -1, -1))

//val seq = Seq(1, 2, 3, 4)
//seq.filter(x => x % 2 == 0)

//var set = Set((1->3, 1->2, 2->3))
//set += ((1->3, 2->3, 1->2))
//set
//
//set.toSeq.permutations foreach println

//
//val graph = new scp.Graph(
//  sc.parallelize(Seq(0, 1, 2, 3)),
//  sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))

//case class Edge(src: Vertex, dst: Vertex) {
//
//}
//
//case class Vertex(vid: Int) extends Ordered[Vertex]{
//  import org.apache.spark.SparkContext
//  import org.apache.spark.rdd.RDD
//
//  private var in_edges: RDD[Edge] = _
//  private var out_edges: RDD[Edge] = _
//
//  def get_in_edges(): RDD[Edge] = {
//    if (in_edges == null) throw new ExceptionInInitializerError("Incoming edges not set for this vertex")
//    else in_edges
//  }
//  def get_out_edges(): RDD[Edge] = {
//    if (out_edges == null) throw new ExceptionInInitializerError("Outgoing edges not set for this vertex")
//    else out_edges
//  }
//  def set_in_edges(inEdges: Seq[Edge]): Unit = {
//    inEdges.map(edge => {
//      if (edge.dst.vid != vid) {
//        throw new IllegalArgumentException("Setting a wrong incoming edge")
//        null
//      }
//    })
//    in_edges = SparkContext.getOrCreate().parallelize(inEdges)
//  }
//  def set_out_edges(outEdges: Seq[Edge]): Unit = {
//    outEdges.map(edge => {
//      if (edge.src.vid != vid) {
//        throw new IllegalArgumentException("Setting a wrong outgoing edge")
//        null
//      }
//    })
//    out_edges = SparkContext.getOrCreate().parallelize(outEdges)
//  }
//
//  def indgr: Long = in_edges.count()
//  def outdgr: Long = out_edges.count()
//
//  def in_vertices: RDD[Vertex] = in_edges.map(x => x.src)
//  def out_vertices: RDD[Vertex] = out_edges.map(x => x.dst)
//
//  override def compare(that: Vertex): Int = this.vid - that.vid
//  //  override def compare(that: Vertex): Int = (outdgr - that.outdgr).toInt
//
//  def canEqual(a: Any): Boolean = a.isInstanceOf[Vertex]
//  override def equals(that: Any): Boolean =
//    that match {
//      case that: Vertex => that.canEqual(this) && this.vid == that.vid
//      case _ => false
//    }
//}
//
//
//val r = new Vertex(1) == new Edge(new Vertex(1), new Vertex(2))

Seq(
  (1, Seq((2, 3), (3, 4))),
  (2, Seq((5,4), (6, 3))),
  (3, Seq((5, 6), (3,3)))
).foreach(x => {
  println(x._1)
  x._2.foreach(xx =>
    println(xx._1 + ", " + xx._2))
})