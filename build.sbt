name := "spark-playground"

publishMavenStyle := true

version := "0.0.1"

scalaVersion := "2.11.6"
scalaVersion in ThisBuild := "2.11.6"

crossScalaVersions := Seq("2.11.6")

//scalacOptions += "-Ypartial-unification"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

val sparkVersion = "2.1.1"
val catsVersion = "1.4.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion ,
  "org.typelevel" %% "cats-core" % catsVersion
)



parallelExecution in Test := false

fork := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Djna.nosys=true")

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalacheck" %% "scalacheck" % "1.13.4",
  "junit" % "junit" % "4.12",
  "junit" % "junit" % "4.11",
  //tag::scalaLogging[]
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  //end::scalaLogging[]
  "org.codehaus.jackson" % "jackson-core-asl" % "1.8.8",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.8",
  "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
  "org.apache.commons" % "commons-math3" % "3.0",
  // https://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12
  "org.slf4j" % "slf4j-log4j12" % "1.2"
)


scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.sonatypeRepo("public")
)

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
