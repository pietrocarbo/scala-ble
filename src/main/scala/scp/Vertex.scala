package scp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class Vertex(vid: Int) extends Ordered[Vertex]{

  var outlinks: Seq[Vertex] = Seq()
  var inlinks: Seq[Vertex] = Seq()
  def links(): Set[Vertex] = outlinks.toSet ++ inlinks.toSet

  def outdegree: Int = outlinks.size
  def indegree: Int = inlinks.size
  def degree: Int = outdegree + indegree

  def this(vid: Int, outNeighbours: Seq[Vertex], inNeighbours: Seq[Vertex]) {
    this(vid)
    outNeighbours.foreach(n => {
      if (n.vid == vid) {
        throw new IllegalArgumentException("Tried to set an outgoing self-edge!")
      }
    })
    inNeighbours.foreach(n => {
      if (n.vid == vid) {
        throw new IllegalArgumentException("Tried to set an ingoing self-edge!")
      }
    })
    this.outlinks = outNeighbours
    this.inlinks = inNeighbours
  }

  def outpairs(): Seq[(Vertex, Vertex)] = {
//    .filter(vs => vs.forall(v => v.outdegree > this.outdegree))
    links().toSeq.combinations(2).map{ case Seq(x, y) => (x, y) }.toSeq
  }

  def is_connected(target: Vertex): Boolean = {
    links().exists(v => v.vid == target.vid)
  }

  override def compare(that: Vertex): Int = outdegree - that.outdegree

  def canEqual(a: Any): Boolean = a.isInstanceOf[Vertex]
  override def equals(that: Any): Boolean =
    that match {
      case that: Vertex => that.canEqual(this) && this.vid == that.vid
      case _ => false
    }
}
