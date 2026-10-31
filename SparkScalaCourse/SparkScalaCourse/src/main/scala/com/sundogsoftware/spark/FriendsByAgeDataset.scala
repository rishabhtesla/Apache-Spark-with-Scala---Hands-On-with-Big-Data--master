package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Compute the average number of friends by age in a social network. */
object FriendsByAgeDataset {

  // Field names/types should match the CSV columns so .as[FakeFriends] can
  // create a typed Dataset after Spark reads the header and schema.
  case class FakeFriends(id: Int, name: String, age: Int, friends: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()

    // header=true uses the first row as column names; inferSchema determines
    // types from the file. .as[FakeFriends] gives the rows a Scala type.
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[FakeFriends]

    // Keep only the two columns needed for this calculation.
    val friendsByAge = ds.select("age", "friends")

    // groupBy creates one group per age; avg computes the mean friend count
    // within each group. Each following line shows a different presentation.
    friendsByAge.groupBy("age").avg("friends").show()

    // Sorted:
    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    // Formatted more nicely:
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()

    // With a custom column name:
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()
  }
}
  