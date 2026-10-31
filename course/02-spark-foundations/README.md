# Module 2: Spark foundations and setup

This module connects the Scala refresher to a running Spark application. Its goal is to make the project layout, execution entry point, and local Spark setup understandable before you study the RDD API.

## What you will learn

- How the Scala source, SBT build, and data files fit together.
- The difference between a `SparkContext` and a `SparkSession` in these examples.
- What `local[*]` means and how a program starts and stops Spark.
- Why input paths depend on the directory from which you launch the program.

## Project map

Run commands from `SparkScalaCourse/SparkScalaCourse/`.

| Path | Purpose |
|---|---|
| `build.sbt` | Declares the Scala version and Spark/Twitter dependencies |
| `src/main/scala/com/sundogsoftware/spark/` | Scala application source |
| `data/` | Input data used by examples |
| `project/build.properties` | SBT version used by the build |

This project declares Scala 2.12.12, Spark 3.0.0, and SBT 1.3.13. It is an older project, so a newer JDK or newer Spark/Scala dependency is not automatically compatible. Start with the project's declared versions; do not upgrade dependencies just to get through a first reading.

## Compile and run

From the repository root:

```bash
cd SparkScalaCourse/SparkScalaCourse
sbt compile
```

Run the smallest Spark program:

```bash
sbt "runMain com.sundogsoftware.spark.HelloWorld"
```

`HelloWorld.scala` loads `data/ml-100k/u.data`, counts its lines, prints the result, and stops its `SparkContext`. The included MovieLens 100K file has 100,000 ratings, so the count is a useful check that the working directory and data file are correct.

If SBT cannot download dependencies, fails to compile, or cannot start Spark, record the exact message first. Check that the selected JDK is compatible with this course-era dependency set before changing application code.

## Follow the program lifecycle

The `main` method is where a normal Scala application begins. In `HelloWorld.scala`, follow these steps:

1. `Logger.getLogger("org").setLevel(Level.ERROR)` reduces Spark log noise.
2. `new SparkContext("local[*]", "HelloWorld")` creates the core RDD interface and tells Spark to run locally using the available CPU cores. The second string is the application name.
3. `sc.textFile(...)` describes reading the file into a distributed collection.
4. `lines.count()` is an action. It requests a result, so Spark executes the work needed to count the records.
5. `sc.stop()` releases the Spark context's resources.

Newer examples use `SparkSession.builder...getOrCreate()` instead. A `SparkSession` is the main entry point for DataFrames, Datasets, SQL, and modern Spark APIs. It also exposes `spark.sparkContext` when an example needs RDD operations. `SparkContext` appears directly in the older RDD programs.

## Understand the path before debugging it

The string `data/ml-100k/u.data` is relative, not an absolute path. When you launch SBT from the project directory, it resolves to `SparkScalaCourse/SparkScalaCourse/data/ml-100k/u.data`. Launching from the repository root would look for a different `data/` directory and can produce a file-not-found error.

The project has some data copies in multiple locations. For consistency, use the data inside the SBT project directory, since that is the directory the source's relative paths expect.

## Practice

1. Run `HelloWorld` and explain why it reports 100,000.
2. Change its application name, run it again, and identify what changes and what does not.
3. Replace `count()` with `take(3)` and print those records. Explain why this is a small result returned to the driver.
4. Find two examples that create `SparkSession` and compare their setup to `HelloWorld`.

## Checkpoint

You are ready to move on when you can compile the project, run `HelloWorld`, explain where its input path resolves, and point out which line causes Spark to execute the read.

**Next:** [Module 3 - RDDs and word count](../03-rdds-and-word-count/README.md)
