import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object tcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    //val (nodes, connections) = (sc.parallelize(Seq(0, 1, 2, 3)),
    //  sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))
    val (nodes, cons) = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/datasets/soc-Epinions1.txt").parseIntoRDDs()
    val connections = cons // .zipWithIndex().filter(x => x._2 <= 20).map(x => x._1)

    // Counted 1624481 in 643.0 seconds
    val triplets = connections.map(x => (x._2, x._1))
      .join(connections)
      .flatMap(x => Seq((x._2._1, x),(x._2._2, x)))
      .join(connections)
      .filter(x => (x._1 == x._2._1._2._1 && x._2._1._2._2 == x._2._2)
        ||
        (x._1 == x._2._1._2._2 && x._2._1._2._1 == x._2._2)
      ).map(x => {
      val c = if (x._1 == x._2._1._2._1) 1.0f else 1.0f / 3
      (Set(x._2._1._1, x._2._1._2._1, x._2._1._2._2), c)
    }).reduceByKey(_+_)

    val (tcount, secs) = Timer.time(triplets.count())
    println(s"Counted $tcount in $secs seconds")

    triplets.repartition(1).saveAsTextFile("tcount")

  }

}
