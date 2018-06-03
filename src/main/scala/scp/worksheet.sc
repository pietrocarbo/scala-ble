"# Nodes: 75879 Edges: 508837".startsWith("#")

import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf()
conf.setMaster("local[*]")
conf.setAppName("Simple Test Application")
val sc = new SparkContext(conf)
sc.textFile("/home/pietro/Desktop/Scalable and Cloud Programming/ScalaSparkProject/resources/soc-Epinions1.txt")