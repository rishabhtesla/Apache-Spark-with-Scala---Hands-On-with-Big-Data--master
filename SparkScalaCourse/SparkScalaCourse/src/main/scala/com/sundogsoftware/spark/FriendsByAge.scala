package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String): (Int, Int) = {
      // Split by commas
      val fields = line.split(",")
      // The input columns are id, name, age, friends; index 0 is the first
      // column. Convert the two fields needed for this analysis to integers.
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      // Return (key, value): age is the grouping key; friends is its value.
      (age, numFriends)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // Starting shape: (age, friends). mapValues keeps each age key and
    // changes its value into (friends, 1), where 1 counts this input row.
    // reduceByKey adds both tuple fields for every record with the same age,
    // producing (age, (totalFriends, numberOfPeople)).
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    // Divide the two totals for each age. Because both are Ints, this uses
    // integer division and drops any fractional part.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    
    // collect is an action: it executes the RDD plan and copies every result
    // to the driver. This is appropriate for the small age summary.
    val results = averagesByAge.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }
    
}
  