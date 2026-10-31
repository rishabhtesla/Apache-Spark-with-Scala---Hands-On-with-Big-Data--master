# Module 6: Machine learning with Spark

This module introduces the shape of machine-learning work in Spark: prepare structured input, define the fields an algorithm expects, fit a model, and inspect its predictions or recommendations. The goal is to understand the data flow, not to treat an algorithm as a black box.

## What you will learn

- The difference between a feature (input) and a label (the value to predict).
- How Spark MLlib expects features and labels to be represented.
- How a model is trained and used for prediction.
- What collaborative filtering and movie similarity mean at a high level.

## Read the examples

Open these files in `src/main/scala/com/sundogsoftware/spark/`:

- `LinearRegressionDataFrame.scala` and `LinearRegressionDataFrameDataset.scala` - predict a numeric label from numeric features using `data/regression.txt`.
- `MovieRecommendationsALS.scala` and `MovieRecommendationsALSDataset.scala` - train an Alternating Least Squares (ALS) collaborative-filtering model from MovieLens ratings.
- `MovieSimilarities.scala`, `MovieSimilarities1M.scala`, `MovieSimilaritiesDataset.scala`, `MovieSimilarities1MDataset.scala`, and `MovieSimilarities1MDatasetLocal.scala` - construct movie pairs and compute similarity scores.

The 100K MovieLens data is under `data/ml-100k/`; the million-rating data is under `data/ml-1m/`. The matching data documentation explains the input columns.

## Start with regression

In `LinearRegressionDataFrame.scala`, the code converts each input row into `(label, features)`. A **label** is the known target value; **features** are the input measurements used to predict it. Spark ML's linear regression estimator expects these fields, with features represented by a Spark ML vector.

The program splits data into training and test sets, fits `LinearRegression` on the training set, then calls `model.transform(testDF)` to add predictions. Compare each prediction to the known label. The source does not calculate an evaluation metric, so the printed pairs alone are not a complete assessment of model quality.

The split is random, so which examples appear in each half can change between runs unless a seed is set. Do not expect every printed prediction to be identical across runs.

## Then study recommendations and similarity

ALS learns latent representations from user-item ratings. `MovieRecommendationsALS.scala` turns MovieLens rows into `(userId, movieId, rating)` records, fits an ALS model, and asks for recommendations for one user. It reads a user ID from the first command-line argument:

```bash
sbt "runMain com.sundogsoftware.spark.MovieRecommendationsALS 1"
```

The ID must exist in the dataset. The Dataset version is `MovieRecommendationsALSDataset.scala`; compare the input types and column names before comparing the model steps.

The movie-similarity examples take a different route: find pairs of movies rated by the same user, aggregate rating products, and calculate cosine similarity. Some versions accept a movie ID as an argument. For example:

```bash
sbt "runMain com.sundogsoftware.spark.MovieSimilaritiesDataset 50"
```

The exact output depends on the selected dataset and thresholds in that source file. The 1M-rating examples process substantially more data than the 100K examples, so begin with the smaller dataset when learning the code path.

## Practice

1. In the regression example, trace the raw columns to `label` and `features`. Change one step and check the types still match what the estimator expects.
2. Add a reproducible seed to the train/test split, then verify repeated runs use the same split.
3. Print a simple error for each regression prediction, such as `prediction - label`, and explain its sign.
4. In the ALS example, identify the user, item, and rating columns and compare them to the dataset passed to `fit`.
5. In a movie similarity program, trace how two movies become a pair and identify what makes the similarity score stronger or weaker.

## Checkpoint

You are ready to move on when you can explain the input columns, model API, training operation, and what the program does with the model's output. Remember that runnable code is not automatically a reliable or production-ready model evaluation.

**Next:** [Module 7 - Streaming and graph processing](../07-streaming-and-graphs/README.md)
