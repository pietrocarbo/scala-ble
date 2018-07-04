import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
conf.setAppName("BattilanaSparkApp")
conf.setMaster("local[*]")
val sc = new SparkContext(conf)

val (nodes, connections) = (sc.parallelize(Seq(0, 1, 2, 3)),
  sc.parallelize(Seq
  (0 -> 1, 1 -> 0, 1 -> 2, 2 -> 3, 3 -> 0, 3 -> 1, 0 -> 3, 1 -> 3, 3 -> 2)))

val links = connections.map(x => (Set(x._1, x._2), 1))
  .reduceByKey(_ + _).persist()
links.foreach(println(_))

val friends = links.filter(x => x._2 == 2).map(x => x._1)
friends.foreach(println(_))

val friends_lists = friends.map(x => x.toList)
  .map {
    case List(a, b) => (a, b)
  }
  .flatMap(x => List(x, x.swap))
  .groupByKey()
friends_lists.foreach(println(_))

val mutual_friends = friends_lists.map {
  case (person, its_friends) =>
    (person, its_friends ++ Iterable[Int](person))
}.flatMap {
  case (person, its_friends) =>
    val ac = its_friends.toSeq.combinations(2)
    ac.map(x => if (x.head == person || x(1) == person)
      (Set(x.head, x(1)), (0, person))
    else
      (Set(x.head, x(1)), (1, person))
    )
}
  .groupByKey()
  .filter(x => x._2.forall(xx => xx._1 != 0))
  .map(x =>
    (x._1.toSeq,
     x._2.map(x => (x._1, Set(x._2)))
       .reduce((x, y) => (x._1 + y._1, x._2 ++ y._2))))
  .flatMap(x =>
    Seq((x._1.head, (x._1(1), x._2._1, x._2._2)), (x._1(1), (x._1.head, x._2._1, x._2._2)))
  )
  .groupByKey()
  .mapValues(x => x.toSeq.sortBy(_._2)(Ordering[Int].reverse))
mutual_friends.foreach(println(_))

sc.stop()