name := "wiki-spark"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVersion = "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP4" % Test
)
        