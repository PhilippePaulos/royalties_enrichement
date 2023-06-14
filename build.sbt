ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.11"

// Define common settings for all projects
val commonSettings = Seq(
  organization := "com.believe",
  scalaVersion := "2.13.11",
  scalacOptions := Seq("-deprecation", "-feature", "-unchecked", "-Xlint")
)

// Library versions
val spark = "3.3.2"
val delta = "2.2.0"
val scalatest = "3.2.16"
val log4jScala = "12.0"
val log4jCore = "2.20.0"
val snakeyaml = "2.0"
val mockito = "3.2.16.0"

// Project dependencies
val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % spark % Provided,
  "org.apache.spark" %% "spark-sql" % spark % Provided,
  "io.delta" %% "delta-core" % delta % Provided,
  "org.apache.logging.log4j" %% "log4j-api-scala" % log4jScala,
  "org.apache.logging.log4j" % "log4j-core" % log4jCore,
  "org.yaml" % "snakeyaml" % snakeyaml,
  "org.scalactic" %% "scalactic" % scalatest % Test,
  "org.scalatest" %% "scalatest" % scalatest % Test,
  "org.scalatestplus" %% "mockito-4-11" % mockito % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "royalties_ingestion",
    commonSettings,
    libraryDependencies ++= dependencies
  )
