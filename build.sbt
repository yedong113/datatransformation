name := "datatransformation"

version := "0.1"

scalaVersion := "2.11.4"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.4"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.4"


val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-launcher_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-network-shuffle_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test
libraryDependencies += "org.scalacheck" %"scalacheck_2.11" % "1.14.0" % Test
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.15"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"


libraryDependencies ++= {
   Seq(
      "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.7.0",
      "ch.qos.logback" % "logback-classic" % "1.1.2",
      "org.scalatest" %% "scalatest" % "2.2.4" % Test
   )
}

