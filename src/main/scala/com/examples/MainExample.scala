package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

/*
spark-submit --class com.examples.MainExample \
  --master local \
  /Users/sethangavel/maven/spark-scala-maven-boilerplate-project/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  "/Users/sethangavel/datasets/sparkDatasets/words.txt" \
  "/Users/sethangavel/datasets/sparkDatasets/output"
 */
object MainExample {

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample <path-to-files> <output-path>")
      System.exit(1)
    }

    val jobName = "MainExample"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val outputPath = arg(1)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    val files = sc.textFile(pathToFiles)

    // do your work here
    val rowsWithoutSpaces = files.map(_.replaceAll(" ", ","))

    // and save the result
    rowsWithoutSpaces.saveAsTextFile(outputPath)

  }
}
