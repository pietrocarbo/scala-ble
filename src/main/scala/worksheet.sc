//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.{SparkConf, SparkContext}
//
//val sc = new SparkContext(
//  new SparkConf().setMaster("local[*]")
//    .setAppName("BattilanaSparkApp"))
//
//val (nodes, connections) = (sc.parallelize(Seq(0, 1, 2, 3)),
//  sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))
//
//val conns: connections.type = connections.persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//val out = conns.groupBy(x => x._2)
//  .map(x => (x._1, x._2.map(x => x._1)))
//  .join(conns.groupByKey())
//  .map(v => {
//      s"${v._2._1.map(identity)} -> " +
//      s"${v._1} -> " +
//      s"${v._2._2.map(identity)}"
//  })
//
//out.foreach(println(_))
//
//sc.stop()

1.0f / Math.pow(10, 4)