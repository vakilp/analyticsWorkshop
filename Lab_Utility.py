# Databricks notebook source
import zipfile
with zipfile.ZipFile("./LoanStats_2018Q2.csv.zip", 'r') as zip_ref:
    zip_ref.extractall("/dbfs/tmp/")


# COMMAND ----------

username = (spark.sql("SELECT current_user() as user").collect()[0]["user"])
userhome = "dbfs:/home/"+username

# COMMAND ----------

dbutils.fs.cp("dbfs:/tmp/LoanStats_2018Q2.csv", userhome+"/LoanStats_2018Q2.csv")
