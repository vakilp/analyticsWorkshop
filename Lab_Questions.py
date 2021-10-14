# Databricks notebook source
dbutils.widgets.text("FilePath", "dbfs:/home/" + (spark.sql("SELECT current_user() as user").collect()[0]["user"]) + "/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Loan Data Set Analysis
# MAGIC 
# MAGIC This loan data set is available on [Lending Club](https://www.lendingclub.com/info/download-data.action)
# MAGIC 
# MAGIC In this Lab we will accomplish the following tasks:
# MAGIC 
# MAGIC 1. ETL - Read CSV file from S3, do some transformations and write out to Delta
# MAGIC 2. SQL Analytics - Read in delta files and do analytics and visualization against data
# MAGIC 3. Machine Learning - Run a logistic regression algorithm to predict which loans will likely go into collections

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##ETL

# COMMAND ----------

# MAGIC %md
# MAGIC In the Widget "File Path" set the path you will use to read and write the files you will process in this workshop. A path might be "dbfs:/home/< username >"

# COMMAND ----------

filePath = dbutils.widgets.get("FilePath")
filePath

# COMMAND ----------

# MAGIC %md ### Reading and Writing data - S3

# COMMAND ----------

# MAGIC %md ##### The main function to read data is the spark.read function. To get information on it, try help(spark.read).

# COMMAND ----------

help(spark.read)

# COMMAND ----------

# MAGIC %md <img src="http://www.hireauthority.com/wp-content/uploads/2015/02/Hot_Tip.jpg", width="100px", height="100px">
# MAGIC try help(spark.read.FILETYPE) to narrow the help result down to the file type you're interested in

# COMMAND ----------

# MAGIC %md #### Part 1: DBFS
# MAGIC The Databricks File System or DBFS is a distributed file system that comes installed on Spark Clusters in Databricks. It is a layer over S3, which allows you to:
# MAGIC 1. Cache S3 data on the solid-state disks (SSDs) of your worker nodes to speed up access.
# MAGIC 2. Persist files to S3 in DBFS, so you won’t lose data even after you terminate the clusters.
# MAGIC 
# MAGIC You can access DBFS using the [dbutils package](https://docs.databricks.com/user-guide/dbutils.html#dbutils) (already on your cluster). To get info on dbutils, try `dbutils.fs.help()`. The commands should seem familiar - they are standard file system commands you'll see when moving files around in terminal.

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC 
# MAGIC 1. List the files in the s3 bucket folder `/databricks-datasets` using the `dbutils.fs.ls` method

# COMMAND ----------

# write your code here


# COMMAND ----------

# MAGIC %md #### Part 2: Reading from csv files
# MAGIC when reading from csvs, there are a few options we need to set -
# MAGIC 1. **schema**: if we have a schema defined for the csv, we can pass that to the reader to avoid having to infer the schema
# MAGIC 2. **sep**: the character used as a separator (usually ',')
# MAGIC 3. **inferSchema**: whether or not we need the reader to infer the schema. If true, the reader has to go through the input once to determine the schema, which can make it *slow*
# MAGIC 4. **header**: whether or not our csv contains a header row
# MAGIC 
# MAGIC In Python...
# MAGIC 
# MAGIC ```
# MAGIC df = spark.read.csv('/path/to/file', header=True, inferSchema=True)
# MAGIC ```
# MAGIC 
# MAGIC Or in SQL...
# MAGIC 
# MAGIC ```
# MAGIC CREATE OR REPLACE TEMPORARY VIEW myview
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   path '/path/to/file',
# MAGIC   header true,
# MAGIC   inferSchema true
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC 
# MAGIC Load the csv file `LoanStats_2018Q2.csv` from the `/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv` folder into a PySpark DataFrame or SQL view called `loanstats_df` (make sure to use the `inferSchema` and `header` options)

# COMMAND ----------

# DBTITLE 1,Python Solution
#Python solution


# COMMAND ----------

# DBTITLE 1,SQL Solution
# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC How many rows does the `loanstats_df` dataframe have?

# COMMAND ----------

# DBTITLE 1,Python Solution
#Python solution


# COMMAND ----------

# DBTITLE 1,SQL Solution
# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC What is the Schema of the dataframe/table?

# COMMAND ----------

# DBTITLE 1,Python Solution
#Python solution


# COMMAND ----------

# DBTITLE 1,SQL Solution
# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 03: ETL to Delta
# MAGIC 
# MAGIC The source data is arriving in CSV format. However, Delta is the optimized format to store our data in. The reasons for this are multi-fold some of which are discussed below:
# MAGIC 
# MAGIC 1. Delta saves data as Parquet files which stores the data in a columnar format. This means that when we run filters against specific columns, we identify the rows that have to be read and returned off of just one column. In a CSV file, we would have to read each column of each row fully to determine whether it has to be returned. This saves a lot of read cycles.
# MAGIC 
# MAGIC 2. All data in Delta Lake is stored in Apache Parquet format enabling Delta Lake to leverage the efficient compression and encoding schemes that are native to Parquet.
# MAGIC 
# MAGIC 3. Delta brings ACID transactions to your data lakes. It provides serializability, the strongest level of isolation level. Delta Lake provides snapshots of data enabling developers to access and revert to earlier versions of data for audits, rollbacks or to reproduce experiments. Delta Lake provides the ability to specify your schema and enforce it. This helps ensure that the data types are correct and required columns are present, preventing bad data from causing data corruption.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We have determined that the columns that are going to be of use for analytics and prediction downstream are as follows:
# MAGIC 
# MAGIC loan_amnt
# MAGIC 
# MAGIC term
# MAGIC 
# MAGIC int_rate
# MAGIC 
# MAGIC installment
# MAGIC 
# MAGIC grade
# MAGIC 
# MAGIC emp_length
# MAGIC 
# MAGIC annual_inc
# MAGIC 
# MAGIC addr_state
# MAGIC 
# MAGIC dti
# MAGIC 
# MAGIC delinq_2yrs
# MAGIC 
# MAGIC inq_last_6mths
# MAGIC 
# MAGIC mths_since_last_delinq
# MAGIC 
# MAGIC revol_bal
# MAGIC 
# MAGIC revol_util
# MAGIC 
# MAGIC out_prncp
# MAGIC 
# MAGIC total_pymnt
# MAGIC 
# MAGIC total_rec_prncp
# MAGIC 
# MAGIC total_rec_int
# MAGIC 
# MAGIC collections_12_mths_ex_med

# COMMAND ----------

#Python solution


# COMMAND ----------

# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Data Cleaning and munging
# MAGIC Now, there are a few things we must fix before we can start using this data for ML where we try to predict whether a given loan will likely go into collections or not.
# MAGIC 
# MAGIC Lets print the schema first. We know that all columns need to be numeric to be used as features in MLLib.

# COMMAND ----------

#Python solution


# COMMAND ----------

# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We see that there are plenty of non-numeric columns. Let us start with term and replicate that process where it makes sense

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC 
# MAGIC Take the ```term``` column in ```loanstats_newDF``` and do the following:
# MAGIC 
# MAGIC 1. Add a new column to data frame ```newTerm```
# MAGIC 2. ```newTerm``` should only have the integer associated with the term
# MAGIC 3. ```newTerm``` should be Integer Type
# MAGIC 4. Drop the column ```term```
# MAGIC 5. The output of this transformation should be a new dataframe/temporary view called ```loanstats_etlDF```
# MAGIC 
# MAGIC ####Hints
# MAGIC 
# MAGIC 1. You will need to ```import pyspark.sql.functions as f```
# MAGIC 2. You will need to ```from pyspark.sql.types import IntegerType``` 
# MAGIC 3. Note that the character that is seperating the number of months from the string "months" is a space
# MAGIC 4. You will have to use Python slice Operation
# MAGIC 5. You will have to use PySpark's withColumn operator on the loanstats_newDF dataframe
# MAGIC 6. You will have to use drop operator on the loanstats_newDF dataframe

# COMMAND ----------

# DBTITLE 1,Python Code
#Python solution


# COMMAND ----------

# DBTITLE 1,SQL Code
# MAGIC %sql --write you sql code here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We will do similar processing with the int_rate and revol_util columns

# COMMAND ----------

loanstats_etlDF = (loanstats_newDF
                  .withColumn("newTerm", f.split("term", " ")[1].cast(IntegerType()))
                  .drop("term")
                  .withColumn("intRate", f.split("int_rate", "%")[0].cast(DoubleType()))
                  .drop("int_rate")
                   .withColumn("revolUtil", f.split("revol_util", "%")[0].cast(DoubleType()))
                  .drop("revol_util")
                  )
display(loanstats_etlDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temporary view loanstats_etlDF as
# MAGIC select  loan_amnt,installment,grade,emp_length,annual_inc,addr_state,dti,delinq_2yrs,inq_last_6mths,mths_since_last_delinq,revol_bal,revol_util,out_prncp,total_pymnt,total_rec_prncp,total_rec_int, collections_12_mths_ex_med,
# MAGIC cast(split(term, ' ')[1] as int) as newTerm,
# MAGIC cast(split(revol_util, '%')[0] as double) as revolUtil,
# MAGIC cast(split(int_rate, '%')[0] as double) as intRate  from loanstats_newDF;
# MAGIC 
# MAGIC select * from loanstats_etlDF;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Write to Delta File
# MAGIC 
# MAGIC To write a file out, you can look for help using help(loanstats_etlDF.write)

# COMMAND ----------

help(loanstats_etlDF.write)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In Python
# MAGIC 
# MAGIC ```
# MAGIC df.write.format("delta").save("path_to_file")
# MAGIC ```
# MAGIC 
# MAGIC In SQL
# MAGIC 
# MAGIC ```
# MAGIC CREATE DATABASE IF NOT EXISTS mydb;
# MAGIC 
# MAGIC CREATE TABLE mydb.mytable
# MAGIC USING delta
# MAGIC OPTIONS (
# MAGIC   path 'dbfs:/<user_name>/delta/loanData/',
# MAGIC )
# MAGIC AS
# MAGIC SELECT * from myquery
# MAGIC ```

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC 
# MAGIC Write ```loanstats_etlDF``` to Delta file path

# COMMAND ----------

deltaPath = filePath + "delta/loanData/"
dbutils.widgets.text("DeltaPath", deltaPath)

# COMMAND ----------

# DBTITLE 1,Python Code
loanstats_etlDF.write.format("delta").mode("overwrite").save(deltaPath)

# COMMAND ----------

# DBTITLE 1,SQL Code
# MAGIC %sql
# MAGIC 
# MAGIC create database if not exists parthdb;
# MAGIC 
# MAGIC drop table if exists parthdb.sqlWrite;
# MAGIC 
# MAGIC create table parthdb.sqlWrite
# MAGIC using delta
# MAGIC 
# MAGIC select * from loanstats_etlDF

# COMMAND ----------

display(dbutils.fs.ls(deltaPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Part 4: Analytics
# MAGIC 
# MAGIC Lets do some analytics against the data to get better acquainted with it. We will start by reading the optimized version that we saved in the ETL step.

# COMMAND ----------

df = spark.read.format('delta').load(deltaPath)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temporary view df
# MAGIC using delta
# MAGIC options (
# MAGIC   path '$DeltaPath'
# MAGIC );
# MAGIC 
# MAGIC select count(*) from df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The beauty of SparkSQL and Dataframes is that you can write the code you are used to against the Spark Data structures but they execute in a distributed fashion unbeknownst to you. For e.g.
# MAGIC 
# MAGIC In Python
# MAGIC 
# MAGIC ```
# MAGIC df.groupBy(df["columnName"]).count()
# MAGIC ```
# MAGIC Or in SQL
# MAGIC ```
# MAGIC CREATE OR REPLACE TEMPORARY VIEW groupedView
# MAGIC select count(*) from tableName
# MAGIC group by columnName;
# MAGIC select * from groupedView
# MAGIC ```
# MAGIC 
# MAGIC You can use where cluases, filters and all transformations available to you in ANSI 2003 SQL on top of the data. This enables an easy transition from the analytics a user runs against Data warehouses to run in a distributed fashion

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC 
# MAGIC The Loan statistics data has a column ```collections_12_mths_ex_med```. This column indicates "Number of collections in 12 months excluding medical collections"
# MAGIC 
# MAGIC 
# MAGIC Find all loans made where number of collections in past 12 months is NOT 0

# COMMAND ----------

# DBTITLE 1,Python Solution
#Python solution

# COMMAND ----------

# DBTITLE 1,SQL Solution
# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://databricks.com/wp-content/themes/databricks/assets/images/spark_logo_2x.png?v=1634081425) **Exercise**
# MAGIC 
# MAGIC Next, provide a distribution of how these loans with collections are distributed across the country.
# MAGIC 
# MAGIC Note that the built in visualizations within Databricks makes this very easy. The data set has a column ```addr_state``` and the built in visualizations has a plot type called Map. Use these two together to come up with the solution

# COMMAND ----------

# DBTITLE 1,Python Solution
#Python solution

# COMMAND ----------

# DBTITLE 1,SQL Solution
# MAGIC %sql --write your code here

# COMMAND ----------

# MAGIC %md ##### If we have particularly large data, we might want to consider partitioning it into smaller pieces when we save.
# MAGIC In non-partitioned tables, Spark has to read all the files in the table's data directory and then apply filters on it. In partitioned tables, there will be subdirectories based on partition column, which distributes the execution load horizontally. We generally want to choose columns we filter or join on for the partition.
# MAGIC ```
# MAGIC df_part = df.repartition("col")
# MAGIC ```
# MAGIC 
# MAGIC To write this data out in the partitioned format, we add `.partitionBy('col')` to our write statement
# MAGIC ```
# MAGIC df.write.partitionBy('col).format...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Part 5: Machine Learning
# MAGIC 
# MAGIC From here on forward, we will focus on the Python APIs for ML algorithm featurization and training. We will also drop some columns that we do not intend to work with in the ML training.
# MAGIC 
# MAGIC As such, this is a multi-class classification problem because collections_12_mths_ex_med indicates how many months a borrower has been delinquent. However, we will convert this into a binary classification problem simply by making all instances of delinquencies into an instance of delinquency. I.e. we have two classes - Not delinquent (Class 0) and delinquetn (Class 1)

# COMMAND ----------

from pyspark.sql.functions import when
df = df.withColumn("label",when(df.collections_12_mths_ex_med !=0, 1).otherwise(df.collections_12_mths_ex_med))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, we create dataframe of features dropping all rows that have nulls in them.

# COMMAND ----------

featureDF = (
  df.select(
    df.label,
    df.loan_amnt,
    df.installment,
    df.annual_inc,
    df.dti,
    df.revol_bal,
    df.out_prncp,
    df.total_pymnt,
    df.total_rec_prncp,
    df.total_rec_int,
    df.intRate,
    df.revolUtil))
featureDF = featureDF.na.drop()

# COMMAND ----------

featureDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We start out by using a Decision tree classifier to see how we perform

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier 
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
    inputCols = ['loan_amnt', 'installment', 'annual_inc', 'dti', 'revol_bal', 'out_prncp', 'total_pymnt', 'total_rec_prncp', 'total_rec_int', 'intRate', 'revolUtil'],
    outputCol = "features")
featuresVec = assembler.transform(featureDF)
featuresVec = featuresVec.select("label","features")
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", seed=1028)

# COMMAND ----------

featuresVec.where(featuresVec.label == 1).count()*100.0/featuresVec.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Example of *Imbalanced Data*, i.e. most algorithms would simply pick Class 0 because that gives us 98.4% precision! To Better predict here, we would have to apply different strategies.

# COMMAND ----------

(trainDF,testDF) = featuresVec.randomSplit([0.7,0.3], seed = 784)

# COMMAND ----------

dtModel = dt.fit(trainDF)
dtPreds = dtModel.transform(testDF)
display(dtPreds)

# COMMAND ----------

modelPath = filePath + "dtModel"
dtModel.write().overwrite().save(modelPath)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.PipelineModel
# MAGIC import org.apache.spark.ml.classification.DecisionTreeClassificationModel
# MAGIC 
# MAGIC val modelPath = dbutils.widgets.get("FilePath") + "dtModel"
# MAGIC val model = DecisionTreeClassificationModel.load(modelPath)
# MAGIC val dtModel = model
# MAGIC display(dtModel)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Logistic Regression gives us a means to weight the classes. Next, we try Logistic Regression setting a class weights column that allows for resampling the data to increase instances of the positive class. 

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

assembler = VectorAssembler(
    inputCols = ['loan_amnt', 'installment', 'annual_inc', 'dti', 'revol_bal', 'out_prncp', 'total_pymnt', 'total_rec_prncp', 'total_rec_int', 'intRate', 'revolUtil'],
    outputCol = "features")
featuresVec = assembler.transform(featureDF)
featuresVec = featuresVec.select("label","features")
#lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
#lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="classWeight")
lr = LogisticRegression()
lr.setFeaturesCol("features").setLabelCol("label").setWeightCol("classWeight")

# COMMAND ----------

datasetSize = featuresVec.count()
class0Count = featuresVec.where(featuresVec.label==0).count()
balancingRatio = (datasetSize-class0Count)*1.0/datasetSize
balancingRatio

# COMMAND ----------

featuresVec = featuresVec.withColumn("classWeight", when(featuresVec.label != 0, 1.0-balancingRatio).otherwise(balancingRatio))
display(featuresVec)

# COMMAND ----------

(trainDF,testDF) = featuresVec.randomSplit([0.7,0.3], seed = 784)

# COMMAND ----------

lrModel = lr.fit(trainDF)
summary = lrModel.summary
summary.precisionByLabel

# COMMAND ----------

evaluation = lrModel.evaluate(testDF)

# COMMAND ----------

display(evaluation.predictions)

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evalBC = BinaryClassificationEvaluator()
roc = evalBC.evaluate(evaluation.predictions)
roc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are various strategies to resolve the imbalance in the data and various [resources](https://machinelearningmastery.com/tactics-to-combat-imbalanced-classes-in-your-machine-learning-dataset/)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Tracking Experiments
# MAGIC 
# MAGIC You can see how it can get difficult to track which algorithms were used, what hyper parameters were tuned and what the resulting performance metrics were. Not only that, but once we get to a stage of having an algorithm we are happy with, we need to package it up so that we can make it reproducible in the future. What about deploying and scaling the ML model? All of these questions are answered by a new Open Source Framework developed by the same folks who developd Spark - [MLflow](https://www.mlflow.org/)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * View your experiments and runs
# MAGIC * Review the parameters and metrics on each run
# MAGIC * Click each run for a detailed view to see the the model, images, and other artifacts produced.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/mlflow/mlflow-ui.gif"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Cleanup Workspace

# COMMAND ----------

dbutils.widgets.remove("FilePath")
dbutils.widgets.remove("DeltaPath")
