name := "SparkDemo"

version := "0.1"

scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

/* Runtime */
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided

libraryDependencies += "org.apache.commons" % "commons-text" % "1.9"

// Spark 3
libraryDependencies += "com.github.mrpowers" %% "spark-stringmetric" % "0.4.0"

// ScalaTest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test

//Machine Learning
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2" % Provided