# Module 5: DataFrames, Datasets, and SQL

RDDs give you low-level control over records. DataFrames and Datasets provide structured columns and schemas, allowing Spark to optimize relational operations such as filtering, grouping, and aggregation. Spark SQL lets you express some of the same operations in SQL syntax.

## What you will learn

- How a schema describes columns and data types.
- How a case class can give a Dataset a Scala type.
- How to select, filter, group, and aggregate structured data.
- How to register a temporary view and query it with SQL.
- How to compare an RDD pipeline to a DataFrame/Dataset pipeline.

## Read the examples in this order

In `src/main/scala/com/sundogsoftware/spark/`:

1. `DataFrames.scala` and `DataFramesDataset.scala` - read people records, inspect schema, select, filter, and group.
2. `SparkSQL.scala` and `SparkSQLDataset.scala` - register people as a temporary view and query teenagers with SQL.
3. `FriendsByAgeDataset.scala` - compute average friends by age with DataFrame expressions.
4. `WordCountDataset.scala` - split text into rows, group by word, and count.
5. `RatingsCounterDataset.scala`, `PopularMoviesDataset.scala`, `PopularMoviesNicerDataset.scala`, and `TotalSpentByCustomerDataset.scala` - compare structured alternatives to pair-RDD analyses.
6. `DegreesOfSeparationDataset.scala` and the `*Dataset` movie similarity/recommendation examples - inspect later, after you are comfortable with the basics.

Run a selected example from the SBT project directory:

```bash
sbt "runMain com.sundogsoftware.spark.DataFramesDataset"
sbt "runMain com.sundogsoftware.spark.SparkSQLDataset"
sbt "runMain com.sundogsoftware.spark.FriendsByAgeDataset"
sbt "runMain com.sundogsoftware.spark.WordCountDataset"
```

## Read the transformations as a table operation

`DataFramesDataset.scala` reads `data/fakefriends.csv`, uses the header and inferred schema options, and maps rows to the `Person` case class. The `.as[Person]` call asks Spark to expose the structured data as a typed Dataset. `import spark.implicits._` makes conversions such as `.as[Person]` and `.toDF()` available.

The next calls read like relational operations:

- `select("name")` selects a column.
- `filter(people("age") < 21)` keeps matching rows.
- `groupBy("age").count()` groups rows and counts them.
- `select(people("name"), people("age") + 10)` builds a result from columns and expressions.

The Dataset API is typed using the Scala case class; a DataFrame is effectively a Dataset of generic rows. SQL and DataFrame expressions can often express the same logical operation.

In `SparkSQLDataset.scala`, `createOrReplaceTempView("people")` registers a view for the lifetime of the Spark session. `spark.sql(...)` runs a SQL query against it. The view is not a permanent database table.

## A data-reading caveat in this snapshot

`DataFrames.scala` reads `fakefriends.csv` as raw text and passes every line to a parser that converts the first field to an integer. The included CSV has a header row (`id,name,age,friends`), so that parser can fail on the header. `DataFramesDataset.scala` is the clearer starting point because it explicitly enables `header` and reads the CSV as structured data. When studying the raw-text version, either account for the header before mapping rows or treat this as a parsing bug to investigate.

## Practice

1. Add a filter for people in a chosen age range, then explain the predicate.
2. Add a column expression and give the output column a useful alias.
3. Write the teenager query once with DataFrame operations and once with SQL.
4. Compare the input type and output type of `WordCount.scala` with `WordCountDataset.scala`.
5. In `FriendsByAgeDataset.scala`, find where the average is rounded and where the result column gets its name.
6. Disable schema inference in a copy of the experiment, inspect the resulting types, then decide when explicit schemas would be safer.

## Checkpoint

Before moving on, explain what a schema gives Spark, what a case class gives the Dataset API, why a temporary view is temporary, and how a DataFrame filter relates to `WHERE` in SQL.

**Next:** [Module 6 - Machine learning](../06-machine-learning/README.md)
