name := "MachineLearning"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"
)