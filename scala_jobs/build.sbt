ThisBuild / scalaVersion := "2.12.18"   // Spark 3.5 is built for Scala 2.12

lazy val root = (project in file("."))
  .settings(
    name := "silver-scala",
    version := "0.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided"
  )