package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object HelloWorld {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // "local[*]" runs Spark on this machine and uses all available CPU cores.
    // A SparkContext is the entry point used by the lower-level RDD API.
    val sc = new SparkContext("local[*]", "HelloWorld")

    // textFile creates an RDD with one String per input line. The path is
    // relative to the project directory from which this program is launched.
    val lines = sc.textFile("data/ml-100k/u.data")

    // count is an action: it makes Spark read the file and return one number
    // to this driver program. Transformations before an action are lazy.
    val numLines = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

    // Stop the context when finished so Spark can release its resources.
    sc.stop()
  }
}
