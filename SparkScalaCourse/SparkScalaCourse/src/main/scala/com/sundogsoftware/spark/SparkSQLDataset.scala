package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataset {

  // Column names and field types should line up with the CSV's data columns.
  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Read the header as column names, infer the data types, and convert the
    // resulting rows to a typed Dataset[Person].
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()
    
    // A temporary view can be queried by name in this SparkSession; it is
    // not a permanent table in a database.
    schemaPeople.createOrReplaceTempView("people")

    // The inclusive age bounds select teenagers from 13 through 19.
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    // collect moves all matching rows from Spark to the driver. Fine for this
    // small file; use show/take or distributed writes for large results.
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}