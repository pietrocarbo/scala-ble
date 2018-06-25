import org.apache.spark.{SparkConf, SparkContext}

object Main {

  type OptionMap = Map[Symbol, Any]

  val usage: String = """
      Usage: main [--save] [--debug] [--triangles] [--pg] [--master URL] graphfile
    """.trim

  def parseCliOptions(map : OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "--master" :: url :: tail => parseCliOptions(map ++ Map('master -> url), tail)
      case "--debug" :: tail => parseCliOptions(map ++ Map('debug -> true), tail)
      case "--toygraph" :: tail => parseCliOptions(map ++ Map('toygraph -> true), tail)
      case "--input" :: value :: tail => parseCliOptions(map ++ Map('input -> value), tail)
      case "--save" :: tail => parseCliOptions(map ++ Map('save -> true), tail)
      case "--triangles" :: tail => parseCliOptions(map ++ Map('triangles -> true), tail)
      case "--pg" :: tail => parseCliOptions(map ++ Map('pg -> true), tail)
      case option :: _ => println("ERROR: Unknown option " + option)
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println(usage)
      sys.exit(1)
    }

    val arglist = args.toList
    val options = parseCliOptions(Map(), arglist)
    println("Parsed options: " + options)

    if (!options.contains('toygraph) && !options.contains('input)) {
      throw new IllegalArgumentException("ERROR: Must specify an input graph filename")
    }

    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    if (options.contains('master))  conf.setMaster(options('master).toString)
    conf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Graph], classOf[Edge], classOf[Vertex]))

    run(conf, options)
  }

  def run(sparkConf: SparkConf, options: OptionMap): Unit = {
    val sc: SparkContext = new SparkContext(sparkConf)
    println("SparkConf is:\n" + sc.getConf.toDebugString)

    val graph =
      if (options.contains('toygraph))
        new Graph(sc.parallelize(Seq(0, 1, 2, 3)),  sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))
      else
        parse_validate_graph(options('input).toString)

    if (options.contains('save)) {
      val (_, secs) = Timer.time(graph.saveAsTextFile(EdgeBoth))
      println(s"Graph representation saved as text file (elapsed $secs seconds)")
    }

    if (options.contains('triangles)) {
      val (tcount, secs) = Timer.time(graph.triangleCount(EdgeBoth))
      println(s"Number of triangles is $tcount (elapsed $secs seconds)")
    }

    if (options.contains('pg)) {
      val (_, secs) = Timer.time(graph.dynamicPageRank())
      println(s"Dynamic page ranking of graph saved as text file (elapsed $secs seconds)")
    }

    sc.stop()
  }

  def parse_validate_graph(input_graph_fn: String): Graph = {
    // TODO: check file exists

    val (nodes, connections) =
      if (input_graph_fn.contains("soc")) {
        new socFileParser(input_graph_fn).parseIntoRDDs()
      }
      else throw new UnsupportedOperationException("Unsupported graph file type")

    // TODO: vertex/edges validation (count, distrinct, ..)
    new Graph(nodes, connections)
  }
}