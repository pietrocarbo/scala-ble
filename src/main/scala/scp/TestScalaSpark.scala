package scp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TestScalaSpark {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Simple Test Application")
    val sc = new SparkContext(conf)

    val logFile = "/home/pietro/Desktop/Scalable and Cloud Programming/wordcount/inputFile.txt"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}