
assemblySettings

name := "WikiClassifier"

organization := "org.apache.predictionio"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
 "org.apache.predictionio" %% "apache-predictionio-core" % "0.10.0-incubating" % "provided",
  "org.apache.spark"        %% "spark-core"               % "1.5.1" % "provided",
 "org.apache.spark"        %% "spark-mllib"              % "1.5.1" % "provided")
