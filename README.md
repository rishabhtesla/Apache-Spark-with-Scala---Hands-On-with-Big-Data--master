# Apache Spark with Scala - A Guided Learning Path

This repository contains the example code and data for *Apache Spark with Scala - Hands On with Big Data!* This guide is an independent, beginner-friendly route through that code. It is written for someone who has seen Scala and Spark before but wants to refresh the ideas, understand what each program is doing, and become confident enough to build a similar solution without copying it.

The course starts with a short Scala refresher, then moves from Spark's execution model through RDDs, pair-RDD analysis, DataFrames/Datasets/SQL, machine learning, and finally streaming and graph processing. Read the module guides in order; each one explains what to look for in the existing source and gives you small changes to try.

## Course modules

| Order | Module | Main ideas |
|---|---|---|
| 1 | [Scala refresher](course/01-scala-refresher/README.md) | Values, types, control flow, functions, tuples, collections, maps |
| 2 | [Spark foundations and setup](course/02-spark-foundations/README.md) | Project layout, SBT, SparkContext, SparkSession, local execution, data paths |
| 3 | [RDDs and word count](course/03-rdds-and-word-count/README.md) | RDDs, lazy transformations, actions, `map`, `flatMap`, `filter`, counting |
| 4 | [Pair RDDs and data analysis](course/04-pair-rdds-and-analysis/README.md) | Keys and values, `reduceByKey`, aggregation, sorting, parsing files |
| 5 | [DataFrames, Datasets, and SQL](course/05-dataframes-datasets-sql/README.md) | Schemas, typed data, relational operations, temporary views, SQL |
| 6 | [Machine learning](course/06-machine-learning/README.md) | Features, labels, regression, collaborative filtering, similarity |
| 7 | [Streaming and graph processing](course/07-streaming-and-graphs/README.md) | Structured Streaming, DStreams, windows, graph edges, degrees of separation |

## What is in this repository?

The SBT project is in `SparkScalaCourse/SparkScalaCourse/`. Its Scala examples are in `src/main/scala/com/sundogsoftware/spark/`, and its input files are in the project's `data/` directory. Run Spark examples from the SBT project directory so relative paths such as `data/book.txt` resolve correctly.

The project is an older course snapshot. Its `build.sbt` declares Scala **2.12.12**, Spark **3.0.0**, and Twitter4J **4.0.4**; its SBT wrapper configuration declares SBT **1.3.13**. These versions are part of the learning context, not a recommendation to use them for a new production application. This checkout compiled with **JDK 11**; its older SBT launcher fails under JDK 26, so use JDK 11 if you encounter launcher or compiler errors on a newer Java version. The guides explain the code as it exists and call out relevant platform or data-format caveats rather than silently changing the examples.

## Start here

1. Read this page once, then begin with [Module 1](course/01-scala-refresher/README.md).
2. Make sure a JDK and SBT are installed and usable in your terminal. The project's older dependencies may constrain which JDK works; if dependency or runtime errors occur, check the project version requirements before changing source code.
3. From the repository root, enter the project and compile:

   ```bash
   cd SparkScalaCourse/SparkScalaCourse
   sbt compile
   ```

4. Run an example by its fully qualified Scala object name. For example:

   ```bash
   sbt "runMain com.sundogsoftware.spark.HelloWorld"
   ```

   The guides give the command for each selected example. Run commands from `SparkScalaCourse/SparkScalaCourse/`, not from the repository root.

## How to study each example

Before running a program, try to answer four questions:

1. What input file or stream does it read?
2. What is one record's shape before and after each transformation?
3. Which operation triggers Spark to do the work?
4. What result should appear, and what parts might vary between runs?

Then read the relevant module guide, run the program, and change one thing at a time. First predict the effect of your change; then run it and explain the result in your own words. Exercises are based on the code already in this repository, so the first task is usually to modify an existing example rather than create a separate project.

## A few important habits

- Spark transformations such as `map` and `filter` describe work; actions such as `count`, `collect`, and `show` trigger work.
- `collect` brings data to the driver process. It is useful for these small learning datasets, but can exhaust driver memory on large data.
- Input paths in these examples are relative to the project directory.
- Some examples expect command-line arguments, credentials, or platform-specific paths. Their module guide identifies these before you run them.
- Do not put real service credentials into source control. The Twitter example reads credentials from `data/twitter.txt`; use only your own credentials and keep them private.
