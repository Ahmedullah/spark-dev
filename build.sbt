// Build file for bulding spark scala code in folder src
name := "SparkDev"
version := "1.0"
scalaVersion := "2.10.5"

val sparkVersion = "1.6.0"

// Base Spark-provided dependencies
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-streaming" % sparkVersion)



