name := "SparkDemo"

version := "0.1"

scalaVersion := "2.12.14"

/* Runtime */
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

libraryDependencies += "org.apache.commons" % "commons-text" % "1.1"

// Spark 3
libraryDependencies += "com.github.mrpowers" %% "spark-stringmetric" % "0.4.0"

