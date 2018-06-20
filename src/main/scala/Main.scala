import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val usage: String = """
      Usage: main [--save] [--debug] [--triangles] [--master URL] graphfile
    """.trim

    def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println(usage)
      sys.exit(1)
    }
    println("Argument from CLI:")
    for (s <- args) {
      println(s)
    }

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--save" :: tail => nextOption(map ++ Map('save -> true), tail)
        case "--debug" :: tail => nextOption(map ++ Map('debug -> true), tail)
        case "--toygraph" :: tail => nextOption(map ++ Map('toygraph -> true), tail)
        case "--input" :: value :: tail => nextOption(map ++ Map('input -> value), tail)
        case "--triangles" :: tail => nextOption(map ++ Map('triangles -> true), tail)
        case "--master" :: url :: tail => nextOption(map ++ Map('master -> url), tail)
        case option :: _ => println("ERROR: Unknown option " + option)
                               sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)
    print("Parsed options ")
    println(options)

    if (!options.contains('toygraph) && !options.contains('input)) {
      println("ERROR: Must specify an input graph")
      sys.exit(1)
    }

    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    if (options.contains('master))  conf.setMaster(options('master).toString)
    conf.set("spark-serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Graph], classOf[Edge], classOf[Vertex]))
    val sc: SparkContext = new SparkContext(conf)
    if (options.contains('debug))  println("Spark Conf is:\n" + sc.getConf.toDebugString)

    val (nodes, edges) = if (options.contains('toygraph)) {
      (sc.parallelize(Seq(0, 1, 2, 3)),
        sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))
    } else {
      // "/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt"
      //    require(nodes.count() == 75879 && edges.count() == 508837, "Error during parsing")
      new socFileParser(options('input).toString).parseIntoRDDs()
    }
    val graph = new Graph(nodes, edges)


    if (options.contains('save)) {
      graph.saveAsTextFile("graph_representation", EdgeBoth)
      println("Graph representation saved as text file")
    }

    if (options.contains('triangles)) {
      val (tcount, secs) = Timer.time(graph.triangleCount(EdgeBoth))
      println(s"Number of triangles is $tcount (elapsed seconds $secs)")
    }

    println("Jobs finished, exiting now.")
    sc.stop()
  }

}