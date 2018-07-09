import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Paths, Files}

object Main {

  var DEBUG: Boolean = false
  var OUTPUT: Boolean =  false
  var N_PARTS: Int =  0

  type OptionMap = Map[Symbol, Any]

  val usageStr: String = """

Usage: ScalaSparkProject.jar [--debug] [--local N_CORES] [--partitions N_PART] [--outputs] [--trasform MODE] [--dpr N_DIGITS] [--friends-rec N_RECS] [--triangles] --input GRAPHFILE

The last four optional arguments are the main program functions also called jobs. If no job is specified as a command-line argument, ALL jobs gets executed.
You can run the application locally and control the number of cores used using the --local option.
Giving the argument '--outputs' the program will save the resulting RDDs in a folder './outputs/jobName'.

OPTIONS
--local N_CORES where 'N_CORES' must be an Integer indicating the number of cores to use. ONLY when launching the application locally.
--partitions N_PARTS where 'N_PARTS' must be an Integer > 0 indicating the number of partitions for the main edges RDD. Recommended value is #cores*#executors*1.75 (powerful flag to tune parallelism, use with caution).
--trasform MODE where 'MODE' must be an Integer between 0 and 2 to indicate the kind of, per-vertex, edges list to get (0: outgoing links, 1: ingoing links, 2: both).
--dpr N_DIGITS where 'N_DIGITS' must be an Integer between 1 and 20 to shift down the decimal digits for the tolerance value (e.g. '--dpr 4' => toll = 1e-4).
--friends-rec N_RECS where 'N_RECS' must be an Integer between 0 and 50 specifying the number of the, per-user/vertex, friends recommendations wanted (nRecs == 0 to emit all recommendations).

 """.trim

  def parseCLIargs(map : OptionMap, args: List[String]) : OptionMap = {
    args match {
      case Nil => map
      case "--debug" :: tail => parseCLIargs(map ++ Map('debug -> true), tail)
      case "--local" :: nCores :: tail => parseCLIargs(map ++ Map('local -> nCores), tail)
      case "--partitions" :: nPart :: tail => parseCLIargs(map ++ Map('parts -> nPart), tail)
      case "--outputs" :: tail => parseCLIargs(map ++ Map('outputs -> true), tail)
      case "--trasform" :: mode :: tail => parseCLIargs(map ++ Map('trasform -> mode), tail)
      case "--dpr" :: toll :: tail => parseCLIargs(map ++ Map('dpr -> toll), tail)
      case "--friends-rec" :: nRecs :: tail => parseCLIargs(map ++ Map('recs -> nRecs), tail)
      case "--triangles" :: tail => parseCLIargs(map ++ Map('triangles -> true), tail)
      case "--input" :: value :: tail => parseCLIargs(map ++ Map('input -> value), tail)
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

    if (options.contains('local)) {
      try {
        if (options('local).toString.toInt < 1 || options('local).toString.toInt > Runtime.getRuntime.availableProcessors()) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'N_CORES' of the --local option must be an integer between 1 and number of cores on your computer" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    if (options.contains('parts)) {
      try {
        if (options('parts).toString.toInt < 0 || options('parts).toString.toInt > Int.MaxValue - 1) throw new Exception()
        N_PARTS = options('parts).toString.toInt
      } catch {
        case e: Exception =>
          println("ERROR: 'N_PARTS' of the --partitions option must be an integer greater than 0" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    if (options.contains('trasform)) {
      try {
         if (options('trasform).toString.toInt < 0 || options('trasform).toString.toInt > 2) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'MODE' of the --trasform job must be an integer between 0 and 2" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    if (options.contains('dpr)) {
      try {
        if (options('dpr).toString.toInt < 1 || options('dpr).toString.toInt > 20) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'N_DIGITS' for the --dpr job must be an integer between 1 and 20" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    if (options.contains('recs)) {
      try {
        if (options('recs).toString.toInt < 0 || options('recs).toString.toInt > 50) throw new Exception()
      } catch {
        case e: Exception =>
          println("ERROR: 'N_RECS' for the --friends-rec job must be an integer between 0 and 50" + "\n" + usageStr)
          sys.exit(1)
      }
    }

    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    if (options.contains('local))  conf.setMaster("local[" + options('local).toString + "]")
    conf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[GraphJobs], classOf[GraphParser]))

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

    val connections = new GraphParser(input_graph_fn).parseIntoRDDs(N_PARTS)
    new GraphJobs(connections, options=(DEBUG, OUTPUT))
  }
}