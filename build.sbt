name := "scala-playground"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "2.1.0",

  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "io.chrisdavenport" %% "log4cats-slf4j" % "1.0.1",
)
