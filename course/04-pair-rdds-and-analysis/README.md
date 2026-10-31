# Module 4: Pair RDDs and data analysis

Many analysis tasks reduce records by a key: total spending per customer, ratings per movie, or average friends per age. Spark represents these records as pairs such as `(customerId, amount)`. This module shows how to parse records into pairs and aggregate them.

## What you will learn

- How a pair RDD represents a key and a value.
- How `reduceByKey` combines values for matching keys.
- How to compute an average by carrying both a sum and a count.
- Why keys must be chosen deliberately, and why sorting or collecting has a cost.

## Follow the examples

Open the examples in `src/main/scala/com/sundogsoftware/spark/`:

- `MinTemperatures.scala`, `MaxTemperatures.scala` - parse weather records and find per-location temperature extremes.
- `FriendsByAge.scala` - turn CSV rows into `(age, friends)` pairs and compute average friends by age.
- `RatingsCounter.scala`, `PopularMovies.scala`, `PopularMoviesNicer.scala` - count ratings and rank popular movies, with movie names loaded from a lookup file.
- `TotalSpentByCustomer.scala`, `TotalSpentByCustomerSorted.scala` - aggregate purchase amounts by customer and sort the totals.
- `MostPopularSuperhero.scala` - count comic-book appearances by hero ID and join IDs to names.
- `DegreesOfSeparation.scala` - a graph-traversal example built from RDD operations; the graph concepts are revisited in Module 7.

These examples use files such as `data/1800.csv`, `data/fakefriends-noheader.csv`, `data/customer-orders.csv`, `data/ml-100k/u.data`, and the Marvel data files. Run an example from `SparkScalaCourse/SparkScalaCourse/` so those paths resolve.

Example:

```bash
sbt "runMain com.sundogsoftware.spark.FriendsByAge"
sbt "runMain com.sundogsoftware.spark.TotalSpentByCustomerSorted"
```

## The pair-RDD pattern

`FriendsByAge.scala` is a useful example to trace. Its `parseLine` function converts one comma-separated line into `(age, numFriends)`. The age is the key; friend count is the value.

The program then changes each value into `(numFriends, 1)` and reduces records by age. For one age, the reducer adds both totals:

```text
(age, (friendsForRow1, 1)) + (age, (friendsForRow2, 1))
    -> (age, (sumOfFriends, numberOfRows))
```

Dividing the total friends by the number of rows gives an average. The example uses integer division, so its result drops any fractional part. Try changing the calculation to use a floating-point value if you want to keep decimal places.

`TotalSpentByCustomerSorted.scala` uses the simpler pattern: parse a row into `(customerId, price)`, use `reduceByKey` to sum each customer's prices, flip each pair to `(total, customerId)`, then sort by total.

## Aggregation rules to remember

- A reducer combines two values for the same key. It should be associative and commutative (for example, addition), because Spark can combine values in different orders and on different partitions.
- `mapValues` transforms only the value part of a pair and keeps the key.
- `reduceByKey` performs a distributed aggregation. It may shuffle data so values with the same key can be combined.
- Sorting generally requires data movement. Use it when the question needs an ordered answer, not by habit.
- `collect()` returns every result to the driver. This is safe for the small course datasets, not an assumption to carry into a large job.

## Practice

1. In `FriendsByAge.scala`, produce a decimal average instead of an integer average.
2. In `TotalSpentByCustomerSorted.scala`, sort from largest total to smallest.
3. Change the key in one example and explain how that changes the question being answered.
4. Add a minimum-count condition to a movie popularity result, so a movie with only one rating cannot rank as highly as a broadly rated movie.
5. For `MinTemperatures.scala`, follow the raw input field indexes into the key/value pair. Write down the units and whether the temperature is Fahrenheit or Celsius before changing it.

## Checkpoint

You are ready to move on when you can explain what the key means, what one value means, how the reducer combines values, and what the final action returns.

**Next:** [Module 5 - DataFrames, Datasets, and SQL](../05-dataframes-datasets-sql/README.md)
