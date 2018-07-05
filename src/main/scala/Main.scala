import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Paths, Files}

object Main {

  var DEBUG: Boolean = false
  var OUTPUT: Boolean =  false

  type OptionMap = Map[Symbol, Any]

  val usageStr: String = """

Usage: ScalaSparkProject.jar [--debug] [--master URL] [--outputs] [--trasform mode] [--dpr toll] [--friends-rec nRecs] [--triangles] --input GRAPHFILE

The last four optional arguments are the program functions also called jobs. If no job is specified as a command-line argument, ALL jobs gets executed
If unspecified as command-line argument, the default Spark master URL is 'local[*]'
With the option '--outputs' the program will save the resulting RDD in a folder './outputs/jobName'

OPTIONS
--trasform mode where 'mode' must be an Integer between 0 and 2 to indicate the kind of, per-vertex, edges list to get (0: outgoing links, 1: ingoing links, 2: for both)
--dpr toll where 'toll' must be an Integer between 1 and 9 to shift down the decimal digits for the tollerance value (e.g. --dpr 4 => 1e-4 => 0.00001)
--friends-rec nRecs where 'nRecs' must be an Integer between 0 and 50 specifying the number of the, per-user/vertex, friends recommendations wanted (nRecs == 0 to emit all recommendations)

 """.trim

  def parseCLIargs(map : OptionMap, args: List[String]) : OptionMap = {
    args match {
      case Nil => map
      case "--master" :: url :: tail => parseCLIargs(map ++ Map('master -> url), tail)
      case "--debug" :: tail => parseCLIargs(map ++ Map('debug -> true), tail)
      case "--input" :: value :: tail => parseCLIargs(map ++ Map('input -> value), tail)
      case "--outputs" :: tail => parseCLIargs(map ++ Map('outputs -> true), tail)
      case "--trasform" :: mode :: tail => parseCLIargs(map ++ Map('trasform -> mode), tail)
      case "--dpr" :: toll :: tail => parseCLIargs(map ++ Map('dpr -> toll), tail)
      case "--friends-rec" :: nRecs :: tail => parseCLIargs(map ++ Map('recs -> nRecs), tail)
      case "--triangles" :: tail => parseCLIargs(map ++ Map('triangles -> true), tail)
      case option :: _ => println("ERROR: Unknown option: " + option + "\n" + usageStr)
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      println("ERROR: Input graph filename not passed" + "\n" + usageStr)
      sys.exit(1)
    }
    val options = parseCLIargs(Map(), args.toList)

    DEBUG = options.contains('debug)
    if (DEBUG) println("Parsed options: " + options)

    OUTPUT = options.contains('outputs)

    if (!options.contains('input)) {
      println("ERROR: Input graph filename not passed" + "\n" + usageStr)
      sys.exit(1)
    }

    if (options.contains('trasform)) {
      try {
         if (options('trasform).toString.toInt < 0 || options('trasform).toString.toInt > 2) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'mode' of the --trasform job must be an integer between 0 and 2" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    if (options.contains('dpr)) {
      try {
        if (options('dpr).toString.toInt < 1 || options('dpr).toString.toInt > 9) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'toll' for the --dpr job must be an integer between 1 and 9" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    if (options.contains('recs)) {
      try {
        if (options('recs).toString.toInt < 0 || options('recs).toString.toInt > 50) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'recs' for the --friends-rec job must be an integer between 0 and 50" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    if (options.contains('master))  conf.setMaster(options('master).toString)
    else conf.setMaster("local[*]")
    conf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[GraphJobs]))

    run(conf, options)
  }

  def run(sparkConf: SparkConf, options: OptionMap): Unit = {
    val sc: SparkContext = new SparkContext(sparkConf)
    if (options.contains('debug)) println("Using SparkConf:\n" + sc.getConf.toDebugString)

    val graph = parse_validate_graph(options('input).toString)

    if (!options.contains('trasform) && !options.contains('dpr)
            &&
        !options.contains('recs) && !options.contains('triangles)) {
      graph.allOps()

    } else {

      def compute(tasks: List[(Symbol, Any)]): Unit = {
        tasks match {
          case ('trasform, mode) :: tail =>
            graph.trasform(mode.toString.toInt match { case 0 => EdgeIn case 1 => EdgeOut case _ => EdgeBoth })
            compute(tail)

          case ('dpr, toll) :: tail =>
            graph.dynamicPageRank(toll.toString.toInt)
            compute(tail)

          case ('recs, nRecs) :: tail =>
            graph.friendsRecommendations(nRecs.toString.toInt)
            compute(tail)

          case ('triangles, true) :: tail =>
            graph.triangleCount()
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

  def parse_validate_graph(input_graph_fn: String): GraphJobs = {
    if (!Files.exists(Paths.get(input_graph_fn))) {
      println("ERROR: Input graph file does not exists: " + input_graph_fn)
      sys.exit(1)
    }

    if (!Paths.get(input_graph_fn).getFileName.toString.startsWith("soc")) {
      println("ERROR: Input graph name: " + input_graph_fn + " is not in the 'soc' graph file format (i.e. 'soc-fileName.txt')")
      sys.exit(1)
    }

    val (nodes, connections) = new GraphParser(input_graph_fn).parseIntoRDDs()
    new GraphJobs(nodes, connections, options=(DEBUG, OUTPUT))
  }
}