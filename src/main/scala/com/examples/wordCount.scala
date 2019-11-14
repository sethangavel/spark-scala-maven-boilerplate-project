package com.examples

import org.apache.spark.{SparkConf, SparkContext}
import sys.process._
/*
spark-submit --class com.examples.wordCount \
--master local \
/Users/sethangavel/maven/spark-scala-maven-boilerplate-project/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 */
object wordCount {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count example")
    val sc = new SparkContext(conf)

    val inFile = "/Users/sethangavel/datasets/sparkDatasets/words.txt"

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile(inFile)

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey(_ + _)

    counts.foreach(println)

    println("="*20)
    println("Total words: " + counts.count())
    println("Removing the dir: rm -r /Users/sethangavel/datasets/sparkDatasets/output ")
    println("="*20)
    "rm -r /Users/sethangavel/datasets/sparkDatasets/output".!
    counts.saveAsTextFile("/Users/sethangavel/datasets/sparkDatasets/output")
    sc.stop()

  }
}
