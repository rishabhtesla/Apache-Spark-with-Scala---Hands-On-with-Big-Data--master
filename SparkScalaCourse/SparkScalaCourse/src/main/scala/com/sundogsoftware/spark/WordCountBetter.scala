package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")   
    
    // Load each line as an RDD[String]. This transformation is lazy until a
    // later action (countByValue) requests the results.
    val input = sc.textFile("data/book.txt")
    
    // \\W+ means one or more non-word characters, so punctuation becomes a
    // separator instead of sticking to the beginning/end of a word.
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize case so "Spark" and "spark" are counted together.
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // This action returns the complete count map to the driver. Use a
    // distributed pair-RDD aggregation when the result could be large.
    val wordCounts = lowercaseWords.countByValue()
    
    // Print the results
    wordCounts.foreach(println)
  }
  
}
