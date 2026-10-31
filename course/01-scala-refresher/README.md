# Module 1: Scala refresher

This module refreshes the Scala pieces used throughout the Spark examples. You do not need to relearn programming from the beginning. Instead, use these small scripts to get comfortable reading Scala again before Spark adds distributed execution to the picture.

## What you will learn

- How `val` differs from `var`, and how Scala infers types.
- How conditionals, pattern matching, loops, and expressions work.
- How to define functions and pass functions as values.
- How tuples, lists, and maps represent data.
- How collection operations such as `map`, `filter`, and `reduce` transform local data.

## Read these examples in order

Open the files under `SparkScalaCourse/SparkScalaCourse/src/main/scala/com/sundogsoftware/spark/`:

1. `LearningScala1.sc` - values, variables, basic types, string interpolation, regular expressions, and booleans.
2. `LearningScala2.sc` - `if` expressions, `match`, loops, and blocks.
3. `LearningScala3.sc` - named functions, function types, and anonymous functions (lambdas).
4. `LearningScala4.sc` - tuples, lists, collection operations, and maps.

The `.sc` files are short scripts rather than Spark application entry points, so they do not have a `main` method for `runMain`. Read and run sections in a Scala worksheet or a Scala 2.12 REPL if your IDE/SBT setup provides one. Otherwise, copy a small section into a scratch Scala file and experiment there.

## A guided reading

### Values, types, and interpolation

In `LearningScala1.sc`, `val hello: String = "Hola!"` declares a value whose reference cannot be reassigned. `var helloThere` can be reassigned later. Prefer `val` unless you have a clear reason to change a variable; most Spark transformations are easier to reason about when their inputs are immutable.

Scala can infer a type, so `val numberOne = 1` is normally enough. The explicit `: Int` annotation is useful while learning and for function parameters or public interfaces. String interpolation makes values readable: `s"answer: $numberOne"` inserts a value, while `f"pi: $pi%.3f"` also formats it.

### Expressions and functions

`LearningScala2.sc` shows that an `if` or a block can produce a value. `LearningScala3.sc` defines `squareIt` and `transformInt`. The parameter `f: Int => Int` means that `transformInt` accepts a function that takes an `Int` and returns an `Int`. The expression `x => x * x` is an anonymous function with that shape.

This matters in Spark because calls such as `rdd.map(x => ...)` pass a function to Spark describing how to process each record.

### Collections

In `LearningScala4.sc`, a tuple groups a fixed number of possibly different values, such as `("Picard", "Enterprise-D")`. A list contains an ordered sequence of values of one type. A map associates keys with values.

For `numberList`, the following operations have different purposes:

- `map(f)` makes a new value for every input element.
- `filter(predicate)` keeps only elements that satisfy a test.
- `reduce(f)` combines elements into one result; its function must know how to combine two values.

These are local Scala collection operations here. Spark has operations with similar names on RDDs, but those operate across distributed partitions and have additional execution costs.

## Practice

Try each exercise already marked in the `.sc` files before reading on:

1. Format twice the value of pi to three decimal places.
2. Print the first ten Fibonacci numbers.
3. Write a named function and a lambda that convert text to uppercase.
4. From a list of 1 through 20, keep only values divisible by three, first with a loop and then with `filter`.

For each solution, explain the input type, output type, and whether the original collection/value changed. If you are unsure, add an explicit type annotation and inspect intermediate values with `println`.

## Checkpoint

Before moving on, make sure you can read `map(x => ...)` without treating it as Spark-specific syntax, explain what a `(key, value)` tuple means, and trace the result of a small `filter` followed by a `map`.

**Next:** [Module 2 - Spark foundations and setup](../02-spark-foundations/README.md)
