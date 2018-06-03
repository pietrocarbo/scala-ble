package scp

import org.apache.spark.{SparkConf, SparkContext}

object Driver {

  def createSparkContext(master: String, appName: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName(appName)
    return new SparkContext(conf)
  }

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = createSparkContext(master = "local[*]", appName = "MySparkApp")

    val fp: socFileParser = new socFileParser("/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt")

    fp.parse()


  }

}
