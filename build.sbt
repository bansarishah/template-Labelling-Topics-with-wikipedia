
assemblySettings

name := "WikiClassifier"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.predictionio"    %% "core"          % "0.9.7-SNAPSHOT" % "provided",
  "org.apache.spark" %% "spark-core"    % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.5.1" % "provided"
)
