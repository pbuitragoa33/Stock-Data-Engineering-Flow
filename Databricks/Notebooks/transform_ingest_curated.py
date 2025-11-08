# ----------------------------------------------------------------------------------------------------------------------------
# Script to transform and ingest data from Bronze to Silver layer in Azure Data Lake Storage Gen2 using Databricks and PySpark
# ----------------------------------------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Create Spark session

spark = SparkSession.builder.appName("EjemploPySpark").getOrCreate()


# Load environment variables

load_dotenv()

SAS_TOKEN = os.getenv("SAS_TOKEN")

# ---------------------------------------------------------------------
# WORK WITH AZURE DATA LAKE STORAGE GEN2 USING SAS TOKEN IN DATABRICKS
# ---------------------------------------------------------------------


# SAS Token Configuration 

storage_account_name = "projectdsfinancesa"

sas_token = SAS_TOKEN

# Spark configuration for SAS

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")

spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

print("SAS configuration done.")



# Define origin data

path_bronze = f"abfss://raw@{storage_account_name}.dfs.core.windows.net/raw_data_ohlcv.csv"

# Read csv

df_bronze = (spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(path_bronze))

# Display data

display(df_bronze)


# For this analysis, the Close price is the most relevant information. Let's keep only the Close price

# Close columns 

close_columns = [
    "NVDA4",
    "META9",
    "AMZN14",
    "PANW19",
    "ORCL24",
    "AMD29",
    "GOOG34",
    "MSFT39",
    "AVGO44"
]

df_close = df_bronze.select(*close_columns)

display(df_close)



# Rename columns

df_close = df_close.withColumnRenamed("AMD29", "AMD").withColumnRenamed("NVDA4", "NVDA").withColumnRenamed("META9", "META").withColumnRenamed("AMZN14", "AMZN").withColumnRenamed("PANW19", "PANW").withColumnRenamed("ORCL24", "ORCL").withColumnRenamed("GOOG34", "GOOG").withColumnRenamed("MSFT39", "MSFT").withColumnRenamed("AVGO44", "AVGO")

display(df_close)


# Drop first 2 rows

df_close = df_close.subtract(df_close.limit(2))

display(df_close)



# Save the data in curated (silver)

path_silver = f"abfss://curated@{storage_account_name}.dfs.core.windows.net/curated/ohlcv_close"

# Save the processed data in the curated container with parquet

(df_close.write
 .mode("overwrite")       
 .format("parquet")       
 .save(path_silver))

print("Data successfully saved in curated")