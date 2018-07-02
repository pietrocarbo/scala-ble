import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Paths, Files}

object Main {

  var DEBUG: Boolean = false
  var OUTPUT: Boolean =  false

  type OptionMap = Map[Symbol, Any]

  val usageStr: String = """
      Usage: main [--save-results] [--debug] [--parse] [--dpr] [--friends-rec] [--triangles] [--master URL] --input graphfile
    """.trim

  def parseCLIargs(map : OptionMap, args: List[String]) : OptionMap = {
    if (args.isEmpty) {
      println("ERROR: Input graph filename not passed" + "\n" + usageStr)
      sys.exit(1)
    }
    args match {
      case Nil => map
      case "--master" :: url :: tail => parseCLIargs(map ++ Map('master -> url), tail)
      case "--debug" :: tail => parseCLIargs(map ++ Map('debug -> true), tail)
      case "--input" :: value :: tail => parseCLIargs(map ++ Map('input -> value), tail)
      case "--outputs" :: tail => parseCLIargs(map ++ Map('save -> true), tail)
      case "--parse" :: tail => parseCLIargs(map ++ Map('parse -> true), tail)
      case "--dpr" :: tail => parseCLIargs(map ++ Map('dpr -> true), tail)
      case "--friends-rec" :: tail => parseCLIargs(map ++ Map('recs -> true), tail)
      case "--triangles" :: tail => parseCLIargs(map ++ Map('triangles -> true), tail)
      case option :: _ => println("ERROR: Unknown option: " + option + "\n" + usageStr)
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    val options = parseCLIargs(Map(), args.toList)

    DEBUG = options.contains('debug)
    if (DEBUG) println("Parsed options: " + options)

    OUTPUT = options.contains('save)

    if (!options.contains('input)) {
      println("ERROR: Input graph filename not passed" + "\n" + usageStr)
      sys.exit(1)
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
    if (options.contains('debug)) println("Using SparkConf:\n" + sc.getConf.toDebugString)

    val graph = parse_validate_graph(options('input).toString)

    if (!options.contains('parse) && !options.contains('dpr)
      && !options.contains('recs) && !options.contains('triangles)) {

      // do all ops --> graph.allOps()

    } else {

      def compute(tasks: List[(Symbol, Any)]): Unit = {
        tasks match {
          case ('parse, true) :: tail =>
            println("found pattern and do something")

            val (_, secs) = Timer.time(graph.saveAsTextFile(EdgeBoth))
            println(s"Graph representation saved as text file (elapsed $secs seconds)")

            compute(tail)
          case ('dpr, true) :: tail =>
            println("found pattern and do something")

            val (_, secs) = Timer.time(graph.dynamicPageRank())
            println(s"Dynamic page ranking of graph saved as text file (elapsed $secs seconds)")

            compute(tail)
          case ('recs, true) :: tail =>
            println("found pattern and do something")

            val (tcount, secs) = Timer.time(graph.triangleCount(EdgeBoth))
            println(s"Number of triangles is $tcount (elapsed $secs seconds)")

            compute(tail)
          case ('triangles, true) :: tail =>
            println("found pattern and do something")

            val (tcount, secs) = Timer.time(graph.triangleCount(EdgeBoth))
            println(s"Number of triangles is $tcount (elapsed $secs seconds)")

            compute(tail)
          case (_, _) :: tail =>
            compute(tail)
          case _ => // exit recursion
        }
      }
      compute(options.toList)

    }

    sc.stop()
    sys.exit(0)
  }

  def parse_validate_graph(input_graph_fn: String): Graph = {

    if (!Files.exists(Paths.get(input_graph_fn))) {
      println("ERROR: Input graph file does not exists: " + input_graph_fn)
      sys.exit(1)
    }

    if (input_graph_fn.startsWith("soc")) {
      println("ERROR: Input graph name: " + input_graph_fn + " does not indicate 'soc' graph file format")
      sys.exit(1)
    }

    val (nodes, connections) = new socFileParser(input_graph_fn).parseIntoRDDs()

    new Graph(nodes, connections)
  }
}