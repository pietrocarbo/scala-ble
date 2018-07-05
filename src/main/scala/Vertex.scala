//case class Vertex(vid: Int, inlinks: Option[Iterable[Vertex]], outlinks: Option[Iterable[Vertex]]) {
//
//  inlinks.foreach(vseq => vseq.foreach(v => {
//    if (v.vid == vid) {
//      throw new IllegalArgumentException("Tried to set an outgoing self-edge!")
//    }
//  }))
//  outlinks.foreach(vseq => vseq.foreach(v => {
//    if (v.vid == vid) {
//      throw new IllegalArgumentException("Tried to set an outgoing self-edge!")
//    }
//  }))
//
//  def links(linkType: EdgeDirection): Set[Vertex] = {
//    linkType match {
//      case EdgeIn => inlinks.getOrElse(Set()).toSet
//      case EdgeOut => outlinks.getOrElse(Set()).toSet
//      case EdgeBoth => outlinks.getOrElse(Set()).toSet ++ inlinks.getOrElse(Set()).toSet
//    }
//  }
//
//  def outdegree: Int = outlinks.size
//  def indegree: Int = inlinks.size
//  def degree: Int = outdegree + indegree
//
//  def outpairs(linkType: EdgeDirection): Seq[(Vertex, Vertex)] = {
//    links(linkType).toSeq.combinations(2).filter(vs => vs.forall(v => v.is_lower_degree(this))).map{case Seq(x, y) => (x, y)}.toSeq
//  }
//
//  def is_connected(target: Vertex, linkType: EdgeDirection): Boolean = {
//    links(linkType).exists(v => v.vid == target.vid)
//  }
//
//  def is_lower_degree(that: Vertex): Boolean = {
//    val ddgr: Int = this.degree - that.degree
//    if (ddgr > 0)         false
//    else if (ddgr < 0)    true
//    else {
//      val dvid: Int = this.vid - that.vid
//      if (dvid > 0)         false
//      else if (dvid < 0)    true
//      else                  true
//    }
//  }
//
//  def canEqual(a: Any): Boolean = a.isInstanceOf[Vertex]
//  override def equals(that: Any): Boolean =
//    that match {
//      case that: Vertex => that.canEqual(this) && this.vid == that.vid
//      case _ => false
//    }
//}
