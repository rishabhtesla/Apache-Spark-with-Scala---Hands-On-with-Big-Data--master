package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors

object LinearRegressionDataFrame {
  
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    // MLlib expects a numeric target column named "label" and a Vector
    // column named "features". Each line here is label,feature.
      
    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.
    val inputLines = spark.sparkContext.textFile("data/regression.txt")
    val data = inputLines.map(_.split(",")).map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))
    
    // Give the two tuple fields the names required by the estimator.
    import spark.implicits._
    val colNames = Seq("label", "features")
    val df = data.toDF(colNames: _*)
    
    // Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    // Perhaps you're importing data from a real database. Or you are using structured streaming
    // to get your data.
     
    // The model learns from the training portion and is evaluated on held-out
    // test rows. This split is random, so results can vary between runs.
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    // Configure the estimator: regularization controls model complexity,
    // maxIter limits training steps, and tol is the convergence tolerance.
    val lir = new LinearRegression()
      .setRegParam(0.3) // regularization 
      .setElasticNetParam(0.8) // elastic net mixing
      .setMaxIter(100) // max iterations
      .setTol(1E-6) // convergence tolerance
    
    // fit learns the model parameters from the training rows.
    val model = lir.fit(trainingDF)
    
    // transform applies the learned model to test rows and appends a
    // "prediction" column. cache keeps this result available for reuse.
    val fullPredictions = model.transform(testDF).cache()
    
    // This basically adds a "prediction" column to our testDF dataframe.
    
    // Compare each model prediction with the known label from the test set.
    // This example prints pairs but does not calculate an overall error metric.
    val predictionAndLabel = fullPredictions.select("prediction", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))
    
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    // Stop the session
    spark.stop()

  }
}