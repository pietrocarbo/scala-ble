name := "ScalaSparkProject"
version := "0.1"

scalaVersion := "2.11.4"
val sparkVersion: String = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)
