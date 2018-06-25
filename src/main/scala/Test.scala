import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object Test {
  def main(args: Array[String]): Unit = {

    val outputFile = "pageRank"

    val conf = new SparkConf()
    conf.setAppName("BattilanaSparkApp")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val (nodes, connections) = (sc.parallelize(Seq(0, 1, 2, 3)), sc.parallelize(Seq(0 -> 1, 1 -> 2, 2 -> 3, 3 -> 0, 1 -> 3)))
    val (nodes, connections) = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/datasets/soc-Epinions1.txt").parseIntoRDDs()

    val links = connections.groupByKey().persist()

    var ranks = links.mapValues(v => 1.0)
    var prevRanks = links.mapValues(v => 1.0)
    var errRate = 1e-8

    while (ranks.join(prevRanks).map{ case (v, (r, pr)) => r - pr <  errRate}.reduce(_&&_)) {
      prevRanks = ranks.map(x => (x._1, x._2))

      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, rank)) =>
          uLinks.map(t => (t, rank / uLinks.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).
        mapValues(v => 0.15 + 0.85*v)
    }

    import org.apache.commons.io.FileUtils
    import java.io._

    FileUtils.deleteDirectory(new File(outputFile))
    ranks.sortBy(_._2, ascending = false).repartition(1).saveAsTextFile(outputFile)
  }
}
