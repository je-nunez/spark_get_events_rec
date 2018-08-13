name := "spark_perf"

version := "0.0.1"

scalaVersion := "2.11.12"

javaOptions in run ++= Seq(
    "-Xms4G", "-Xmx4G", "-XX:+UseG1GC"
)


// remove the [info] preffixes given by SBT
outputStrategy        :=   Some(StdoutOutput)


lazy val apacheSparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % apacheSparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % apacheSparkVersion,

  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
)

