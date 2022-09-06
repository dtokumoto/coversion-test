// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Capstone Project: Parsing Nested Data
// MAGIC 
// MAGIC Mount JSON data using DBFS, define and apply a schema, parse fields, and save the cleaned results back to DBFS.
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: Chrome
// MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
// MAGIC * Course: ETL Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
// MAGIC 
// MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
// MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Instructions
// MAGIC 
// MAGIC A common source of data in ETL pipelines is <a href="https://kafka.apache.org/" target="_blank">Apache Kafka</a>, or the managed alternative
// MAGIC <a href="https://aws.amazon.com/kinesis/" target="_blank">Kinesis</a>.
// MAGIC A common data type in these use cases is newline-separated JSON.
// MAGIC 
// MAGIC For this exercise, Tweets were streamed from the <a href="https://developer.twitter.com/en/docs" target="_blank">Twitter firehose API</a> into such an aggregation server and,
// MAGIC from there, dumped into the distributed file system.
// MAGIC 
// MAGIC Use these four exercises to perform ETL on the data in this bucket:  
// MAGIC <br>
// MAGIC 1. Extracting and Exploring the Data
// MAGIC 2. Defining and Applying a Schema
// MAGIC 3. Creating the Tables
// MAGIC 4. Loading the Results

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Extracting and Exploring the Data
// MAGIC 
// MAGIC First, review the data. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Explore the Folder Structure
// MAGIC 
// MAGIC Explore the mount and review the directory structure. Use `%fs ls`.  The data is located in `/mnt/training/twitter/firehose/`

// COMMAND ----------

// ANSWER
val path = "/mnt/training/twitter/firehose/2018/01/10/01"
display(dbutils.fs.ls(path)) // %fs ls evaluates to this

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Explore a Single File
// MAGIC 
// MAGIC > "Premature optimization is the root of all evil." -Sir Tony Hoare
// MAGIC 
// MAGIC There are a few gigabytes of Twitter data available in the directory. Hoare's law about premature optimization is applicable here.  Instead of building a schema for the entire data set and then trying it out, an iterative process is much less error prone and runs much faster. Start by working on a single file before you apply your proof of concept across the entire data set.

// COMMAND ----------

// MAGIC %md
// MAGIC Read a single file.  Start with `twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4`. Find this in `/mnt/training/twitter/firehose/2018/01/08/18/`.  Save the results to the variable `df`.

// COMMAND ----------

// ANSWER

val path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
// path = "/mnt/training/twitter/firehose/2018/*/*/*/*" // Read all the data--be careful with this

val df = spark.read.json(path)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = df.columns

dbTest("ET1-S-08-02-01", 1744, df.count())
dbTest("ET1-S-08-02-02", true, cols.contains("id"))
dbTest("ET1-S-08-02-03", true, cols.contains("text"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC Display the schema.

// COMMAND ----------

// ANSWER
df.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Count the records in the file. Save the result to `dfCount`.

// COMMAND ----------

// ANSWER
val dfCount = df.count

// COMMAND ----------

// TEST - Run this cell to test your solution
dbTest("ET1-S-08-03-01", 1744, dfCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2: Defining and Applying a Schema
// MAGIC 
// MAGIC Applying schemas is especially helpful for data with many fields to sort through. With a complex dataset like this, define a schema **that captures only the relevant fields**.
// MAGIC 
// MAGIC Capture the hashtags and dates from the data to get a sense for Twitter trends. Use the same file as above.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Understanding the Data Model
// MAGIC 
// MAGIC In order to apply structure to semi-structured data, you first must understand the data model.  
// MAGIC 
// MAGIC There are two forms of data models to employ: a relational or non-relational model.<br><br>
// MAGIC * **Relational models** are within the domain of traditional databases. [Normalization](https://en.wikipedia.org/wiki/Database_normalization) is the primary goal of the data model. <br>
// MAGIC * **Non-relational data models** prefer scalability, performance, or flexibility over normalized data.
// MAGIC 
// MAGIC Use the following relational model to define a number of tables to join together on different columns, in order to reconstitute the original data. Regardless of the data model, the ETL principles are roughly the same.
// MAGIC 
// MAGIC Compare the following [Entity-Relationship Diagram](https://en.wikipedia.org/wiki/Entity%E2%80%93relationship_model) to the schema you printed out in the previous step to get a sense for how to populate the tables.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ER-diagram.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Create a Schema for the `Tweet` Table
// MAGIC 
// MAGIC Create a schema for the JSON data to extract just the information that is needed for the `Tweet` table, parsing each of the following fields in the data model:
// MAGIC 
// MAGIC | Field | Type|
// MAGIC |-------|-----|
// MAGIC | tweet_id | integer |
// MAGIC | user_id | integer |
// MAGIC | language | string |
// MAGIC | text | string |
// MAGIC | created_at | string* |
// MAGIC 
// MAGIC *Note: Start with `created_at` as a string. Turn this into a timestamp later.
// MAGIC 
// MAGIC Save the schema to `tweetSchema`, use it to create a DataFrame named `tweetDF`, and use the same file used in the exercise above: `"/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"`.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You might need to reexamine the data schema. <br>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** [Import types from `org.apache.spark.sql.types`](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.package).

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
// path = "/mnt/training/twitter/firehose/2018/*/*/*/*"


val tweetSchema = StructType(List(
  StructField("id", LongType, true),
  StructField("user", StructType(List(
    StructField("id", LongType, true)
  )), true),
  StructField("lang", StringType, true),
  StructField("text", StringType, true),
  StructField("created_at", StringType, true)
))

val tweetDF = spark.read.schema(tweetSchema).json(path)
tweetDF.printSchema()
display(tweetDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
import org.apache.spark.sql.functions.col

val schema = tweetSchema.fieldNames
scala.util.Sorting.quickSort(schema)
val tweetCount = tweetDF.filter(col("id").isNotNull).count

dbTest("ET1-S-08-04-01", "created_at", schema(0))
dbTest("ET1-S-08-04-02", "id", schema(1))
dbTest("ET1-S-08-04-03", 1491, tweetCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Create a Schema for the Remaining Tables
// MAGIC 
// MAGIC Finish off the full schema, save it to `fullTweetSchema`, and use it to create the DataFrame `fullTweetDF`. Your schema should parse all the entities from the ER diagram above.  Remember, smart small, run your code, and then iterate.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.types.{StructField, StructType, ArrayType, StringType, IntegerType, LongType}

val path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
// val path = "/mnt/training/twitter/firehose/2018/*/*/*/*"

val fullTweetSchema = StructType(List(
  StructField("id", LongType, true),
  StructField("user", StructType(List(
    StructField("id", LongType, true),
    StructField("screen_name", StringType, true),
    StructField("location", StringType, true),
    StructField("friends_count", IntegerType, true),
    StructField("followers_count", IntegerType, true),
    StructField("description", StringType, true)
  )), true),
  StructField("entities", StructType(List(
    StructField("hashtags", ArrayType(
      StructType(List(
        StructField("text", StringType, true)
      ))
    ), true),
    StructField("urls", ArrayType(
      StructType(List(
        StructField("url", StringType, true),
        StructField("expanded_url", StringType, true),
        StructField("display_url", StringType, true)
      ))
    ), true) 
  )), true),
  StructField("lang", StringType, true),
  StructField("text", StringType, true),
  StructField("created_at", StringType, true)
))

val fullTweetDF = spark.read.schema(fullTweetSchema).json(path)
fullTweetDF.printSchema()
display(fullTweetDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
import org.apache.spark.sql.functions.col

val schema = fullTweetSchema.fieldNames
scala.util.Sorting.quickSort(schema)
val tweetCount = fullTweetDF.filter(col("id").isNotNull).count

dbTest("ET1-S-08-05-01", "created_at", schema(0))
dbTest("ET1-S-08-05-02", "entities", schema(1))
dbTest("ET1-S-08-05-03", 1491, tweetCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 3: Creating the Tables
// MAGIC 
// MAGIC Apply the schema you defined to create tables that match the relational data model.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Filtering Nulls
// MAGIC 
// MAGIC The Twitter data contains both deletions and tweets.  This is why some records appear as null values. Create a DataFrame called `fullTweetFilteredDF` that filters out the null values.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.col

val fullTweetFilteredDF = fullTweetDF
  .filter(col("id").isNotNull)

display(fullTweetFilteredDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
dbTest("ET1-S-08-06-01", 1491, fullTweetFilteredDF.count)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Creating the `Tweet` Table
// MAGIC 
// MAGIC Twitter uses a non-standard timestamp format that Spark doesn't recognize. Currently the `created_at` column is formatted as a string. Create the `Tweet` table and save it as `tweetDF`. Parse the timestamp column using `unix_timestamp`, and cast the result as `TimestampType`. The timestamp format is `EEE MMM dd HH:mm:ss ZZZZZ yyyy`.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `alias` to alias the name of your columns to the final name you want for them.  
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** `id` corresponds to `tweet_id` and `user.id` corresponds to `user_id`.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{unix_timestamp, col}
import org.apache.spark.sql.types.TimestampType

val timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

val tweetDF = fullTweetFilteredDF.select(col("id").alias("tweetID"), 
  col("user.id").alias("userID"), 
  col("lang").alias("language"),
  col("text"),
  unix_timestamp(col("created_at"), timestampFormat).cast(TimestampType).alias("createdAt")
)

display(tweetDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
import org.apache.spark.sql.types.TimestampType

dbTest("ET1-S-08-07-01", TimestampType, tweetDF.schema("createdAt").dataType)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Creating the Account Table
// MAGIC 
// MAGIC Save the account table as `accountDF`.

// COMMAND ----------

// ANSWER
val accountDF = fullTweetFilteredDF.select(col("user.id").alias("userID"), 
    col("user.screen_name").alias("screenName"),
    col("user.location"),
    col("user.friends_count").alias("friendsCount"),
    col("user.followers_count").alias("followersCount"),
    col("user.description")
)

display(accountDF)

// COMMAND ----------

// TEST - Run this cell to test your solution

val cols = accountDF.columns

dbTest("ET1-S-08-08-01", true, cols.contains("friendsCount"))
dbTest("ET1-S-08-08-02", true, cols.contains("screenName"))
dbTest("ET1-S-08-08-03", 1491, accountDF.count())

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 4: Creating Hashtag and URL Tables Using `explode`
// MAGIC 
// MAGIC Each tweet in the data set contains zero, one, or many URLs and hashtags. Parse these using the `explode` function so that each URL or hashtag has its own row.
// MAGIC 
// MAGIC In this example, `explode` gives one row from the original column `hashtags` for each value in an array. All other columns are left untouched.
// MAGIC 
// MAGIC ```
// MAGIC +---------------+--------------------+----------------+
// MAGIC |     screenName|            hashtags|explodedHashtags|
// MAGIC +---------------+--------------------+----------------+
// MAGIC |        zooeeen|[[Tea], [GoldenGl...|           [Tea]|
// MAGIC |        zooeeen|[[Tea], [GoldenGl...|  [GoldenGlobes]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|         [beats]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|           [90s]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|     [90shiphop]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|           [pac]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|        [legend]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|          [thug]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|         [music]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|     [westcoast]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|        [eminem]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|         [drdre]|
// MAGIC |mannydidthisone|[[beats], [90s], ...|          [trap]|
// MAGIC |  Satish0919995|[[BB11], [BiggBos...|          [BB11]|
// MAGIC |  Satish0919995|[[BB11], [BiggBos...|    [BiggBoss11]|
// MAGIC |  Satish0919995|[[BB11], [BiggBos...| [WeekendKaVaar]|
// MAGIC +---------------+--------------------+----------------+
// MAGIC ```
// MAGIC 
// MAGIC The concept of `explode` is similar to `pivot`.
// MAGIC 
// MAGIC Create the rest of the tables and save them to the following DataFrames:<br><br>
// MAGIC 
// MAGIC * `hashtagDF`
// MAGIC * `urlDF`
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@explode(e:org.apache.spark.sql.Column):org.apache.spark.sql.Column" target="_blank">Find the documents for `explode` here</a>

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{explode, col}

val hashtagDF = fullTweetFilteredDF.select(col("id").alias("tweetID"), 
    explode(col("entities.hashtags.text")).alias("hashtag")
)

val urlDF = (fullTweetFilteredDF.select(col("id").alias("tweetID"), 
      explode(col("entities.urls")).alias("urls"))
  .select(
      col("tweetID"),
      col("urls.url").alias("URL"),
      col("urls.display_url").alias("displayURL"),
      col("urls.expanded_url").alias("expandedURL"))
)

hashtagDF.show()
urlDF.show()

// COMMAND ----------

// TEST - Run this cell to test your solution
val hashtagCols = hashtagDF.columns
val urlCols = urlDF.columns
val hashtagDFCounts = hashtagDF.count
val urlDFCounts = urlDF.count

dbTest("ET1-S-08-09-01", true, hashtagCols.contains("hashtag"))
dbTest("ET1-S-08-09-02", true, urlCols.contains("displayURL"))
dbTest("ET1-S-08-09-03", 394, hashtagDFCounts)
dbTest("ET1-S-08-09-04", 368, urlDFCounts)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 4: Loading the Results
// MAGIC 
// MAGIC Use DBFS as your target warehouse for your transformed data. Save the DataFrames in Parquet format to the following endpoints:  
// MAGIC 
// MAGIC | DataFrame    | Endpoint                                 |
// MAGIC |:-------------|:-----------------------------------------|
// MAGIC | `accountDF`  | `"/tmp/" + username + "/account.parquet"`|
// MAGIC | `tweetDF`    | `"/tmp/" + username + "/tweet.parquet"`  |
// MAGIC | `hashtagDF`  | `"/tmp/" + username + "/hashtag.parquet"`|
// MAGIC | `urlDF`      | `"/tmp/" + username + "/url.parquet"`    |
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you run out of storage in `/tmp`, use `.limit(10)` to limit the size of your DataFrames to 10 records.

// COMMAND ----------

// ANSWER
accountDF.write.mode("overwrite").parquet("/tmp/" + username + "/account.parquet")
tweetDF.write.mode("overwrite").parquet("/tmp/" + username + "/tweet.parquet")
hashtagDF.write.mode("overwrite").parquet("/tmp/" + username + "/hashtag.parquet")
urlDF.write.mode("overwrite").parquet("/tmp/" + username + "/url.parquet")

// COMMAND ----------

// TEST - Run this cell to test your solution
import org.apache.spark.sql.DataFrame

val accountDF = spark.read.parquet("/tmp/" + username + "/account.parquet")
val tweetDF = spark.read.parquet("/tmp/" + username + "/tweet.parquet")
val hashtagDF = spark.read.parquet("/tmp/" + username + "/hashtag.parquet")
val urlDF = spark.read.parquet("/tmp/" + username + "/url.parquet")

dbTest("ET1-S-08-10-01", true, accountDF.isInstanceOf[DataFrame])
dbTest("ET1-S-08-10-02", true, tweetDF.isInstanceOf[DataFrame])
dbTest("ET1-S-08-10-03", true, hashtagDF.isInstanceOf[DataFrame])
dbTest("ET1-S-08-10-04", true, urlDF.isInstanceOf[DataFrame])

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
// MAGIC 
// MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> All done!</h2>
// MAGIC 
// MAGIC Thank you for your participation!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
