# Module 7: Streaming and graph processing

These are two different advanced Spark topics grouped at the end of the course. Streaming processes data that arrives over time; graph processing represents entities and their relationships. Study the streaming examples first, then move to the graph examples.

## Part A: Streaming

### Structured Streaming: access-log files

`StructuredStreaming.scala` reads text files from `data/logs/`, extracts fields from access-log lines with regular expressions, groups records by HTTP status code, and writes the running result to the console.

Run it from `SparkScalaCourse/SparkScalaCourse/`:

```bash
sbt "runMain com.sundogsoftware.spark.StructuredStreaming"
```

The program calls `awaitTermination()`, so it is designed to keep running until you stop it (for example, with Ctrl-C in the terminal). It reads from the directory path `data/logs`; use the log files included there and observe how the query reports status counts.

Follow the code in this order: streaming source (`readStream`), parsing expressions (`regexp_extract`), aggregation (`groupBy(...).count()`), output sink (`writeStream`), then query start and wait. A streaming DataFrame describes an ongoing computation; it is not the same as loading one finite file with `read`.

### DStreams: Twitter hashtags

`PopularHashtags.scala` uses Spark Streaming's older DStream API. It reads Twitter credentials from `data/twitter.txt`, creates one-second batches, extracts hashtags, and calculates counts over a five-minute sliding window.

This example requires valid Twitter credentials and a compatible Twitter streaming dependency. Never commit real credentials. It also sets its checkpoint directory to `C:/checkpoint/`, a Windows-style path that is not appropriate for macOS/Linux; change it to a valid local directory before attempting to run it on those systems. If you do not have the service access or a safe local setup, study the transformations without running this external-service example.

### Streaming practice

1. In `StructuredStreaming.scala`, explain what each regular expression extracts and why the status field is cast to an integer.
2. Change the grouping column from status to method and predict the output shape.
3. Explain the difference between a one-time `show()` action and a `writeStream` query.
4. In `PopularHashtags.scala`, identify the batch interval, window length, and slide interval. Explain why a sliding-window count can include data from more than one batch.

## Part B: Graphs

### Build a graph from the Marvel data

`GraphX.scala` creates vertices from the Marvel names file and edges from the Marvel graph file. A vertex represents a hero; an edge means two heroes co-appeared in a comic. The program builds a GraphX graph, ranks heroes by degree (number of connections), and uses Pregel-style message passing to calculate distance from Spider-Man.

`DegreesOfSeparation.scala` solves a related shortest-path problem using iterative RDD transformations. `DegreesOfSeparationDataset.scala` provides another API version. Compare these programs only after you can identify the graph's vertices, edges, start node, target node, and current distance.

**Filename caveat:** the files in this checkout are named `data/Marvel-names.txt` and `data/Marvel-graph.txt` (capital `M`), while the Scala programs request lower-case `data/marvel-names.txt` and `data/marvel-graph.txt`. The default macOS filesystem is often case-insensitive, which can hide the mismatch. On a case-sensitive filesystem, make the filenames and the paths in the source agree before running these examples.

### Graph practice

1. Explain what one edge means in the Marvel graph and why the input can create multiple neighbors for one hero.
2. In `GraphX.scala`, find the source vertex value, the message sent to neighbors, and the reducer that chooses the smaller distance.
3. In `DegreesOfSeparation.scala`, describe the WHITE/GRAY/BLACK states and what one BFS iteration does.
4. Change the start or target hero ID only after checking that the ID appears in the supplied data.

## Checkpoint

For streaming, be able to identify the input source, transformation, aggregation, output sink, and termination behavior. For graphs, be able to name the vertices and edges, then trace one message across an edge and explain how the shortest distance is retained.

**Course complete:** return to the [course roadmap](../../README.md), choose an example you understand, and implement a related variation from a blank file.
