# Module 3: RDDs and word count

An RDD (Resilient Distributed Dataset) is Spark's distributed collection API. Think of it as a collection split into partitions that Spark can process in parallel. The examples in this module introduce the core workflow: read data, describe transformations, then call an action to get a result.

## What you will learn

- The difference between an RDD transformation and an action.
- How `textFile`, `flatMap`, `map`, and `filter` fit into one data pipeline.
- Why Spark transformations are lazy.
- How several versions of word count progressively change the result.

## Read these files in order

All paths below are relative to the source directory `src/main/scala/com/sundogsoftware/spark/`.

1. `HelloWorld.scala` - the minimal `textFile` and `count` example from Module 2.
2. `WordCount.scala` - split lines into words and count values.
3. `WordCountBetter.scala` - split on non-word characters and normalize case.
4. `WordCountBetterSorted.scala` - build `(word, 1)` pairs, aggregate with `reduceByKey`, then sort by count.
5. `WordCountDataset.scala` - a preview of the same kind of task using Spark SQL operations; study it again in Module 5.

Run the RDD programs from the SBT project directory:

```bash
sbt "runMain com.sundogsoftware.spark.WordCount"
sbt "runMain com.sundogsoftware.spark.WordCountBetter"
sbt "runMain com.sundogsoftware.spark.WordCountBetterSorted"
```

These read `data/book.txt`. The output contains many words; the important thing is to compare how punctuation, capitalization, counting, and ordering differ between programs.

## Trace one record through the pipeline

In `WordCount.scala`:

```scala
val input = sc.textFile("data/book.txt")
val words = input.flatMap(x => x.split(" "))
val wordCounts = words.countByValue()
```

`input` is an RDD of lines. `flatMap` runs a function on each line and flattens the returned arrays, making an RDD of words. `countByValue()` is an action that computes counts and returns a local result to the driver.

In `WordCountBetter.scala`, `split("\\W+")` treats runs of non-word characters as separators, and `toLowerCase()` makes `Spark` and `spark` count as the same word. In `WordCountBetterSorted.scala`, the pipeline explicitly maps each word to `(word, 1)` and combines values with `reduceByKey`. It flips each pair to `(count, word)` to sort by count.

## Lazy work: transformations and actions

`map`, `flatMap`, `filter`, and `reduceByKey` describe transformations. Spark can build a plan from them without immediately reading and processing every record. An action such as `count`, `collect`, `take`, or `countByValue` requests an answer and causes execution.

The distinction is important for debugging and performance. A pipeline can contain several transformations but run only when an action is reached. Some operations, including `reduceByKey` and sorting, may move data between partitions (a shuffle), which can be more expensive than a local map.

`countByValue()` is convenient for this small book, but its result is materialized on the driver. For a larger vocabulary, prefer a distributed key/value aggregation such as `map(word => (word, 1)).reduceByKey(_ + _)`, and only return a suitably small result.

## Practice

1. In `WordCountBetter.scala`, filter out empty tokens and explain why a blank token could appear.
2. Change the separator or normalization and predict which counts will merge.
3. In `WordCountBetterSorted.scala`, sort highest counts first. Which tuple element is the sort key?
4. Add a filter to keep words longer than five characters.
5. Compare the pipeline's transformations to the local `List.map` and `List.filter` from Module 1. What is similar, and what is Spark-specific?

## Checkpoint

For each program, be able to state the record type after every transformation, identify the action that starts computation, and explain which output is returned to the driver.

**Next:** [Module 4 - Pair RDDs and data analysis](../04-pair-rdds-and-analysis/README.md)
