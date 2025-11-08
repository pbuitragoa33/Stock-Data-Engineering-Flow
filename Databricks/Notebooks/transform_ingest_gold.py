# ----------------------------------------------------------------------------------------------------------------------------
# Script to transform and ingest data from Curated to Gold layer in Azure Data Lake Storage Gen2 using Databricks and PySpark
# ----------------------------------------------------------------------------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
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


# Path of the parquet file in the data lake storage

path_silver = f"abfss://curated@{storage_account_name}.dfs.core.windows.net/ohlcv_close/"

# Read the parquet file

df_silver = (spark.read
             .format("parquet")
             .load(path_silver))

# Display the dataframe

df_silver.display()


# The concept of Gold layer is to have data per asset with all the indicators calculated 
# and stored in separate files.

# All the tickers

tickers = ["NVDA", "META", "AMZN", "PANW", "ORCL", "AMD", "GOOG", "MSFT", "AVGO"]

# Storage account name

storage_account_name = "projectdsfinancesa"

# Define the window specification ordered by Date

w_spec = Window.orderBy("Date")

# Build the window specifications

w_10 = w_spec.rowsBetween(-9, 0)
w_20 = w_spec.rowsBetween(-19, 0)
w_50 = w_spec.rowsBetween(-49, 0)
w_100 = w_spec.rowsBetween(-99, 0)

# Bucle for each ticker for each indicator 

for ticker in tickers:

    df_stock = df_silver.select(
        F.col("Date").cast("date"),
        F.col(ticker).cast("double").alias("Close")
    )

    # Indicators

    df_indicators = (df_stock
        .withColumn(f"{ticker}_SMA10", F.avg("Close").over(w_10))
        .withColumn(f"{ticker}_SMA50", F.avg("Close").over(w_50))
        .withColumn(f"{ticker}_SMA100", F.avg("Close").over(w_100))

        # Middle calculations for volatility

        .withColumn("StdDev20", F.stddev("Close").over(w_20))
        .withColumn("Mean20", F.avg("Close").over(w_20))

        # Indicator 1: 20-day Z-Score 
        .withColumn(f"{ticker}_ZScore20", (F.col("Close") - F.col("Mean20")) / F.col("StdDev20"))
        # Indicator 2: ROC (Daily Return)
        .withColumn(f"{ticker}_DailyRet", (F.col("Close") - F.lag("Close", 1).over(w_spec)) / F.lag("Close", 1).over(w_spec))
        # Indicator 3: 20-day Volatility
        .withColumn(f"{ticker}_Volatility20", F.col("StdDev20"))
    )

    # Clean up: Drop the auxiliar columns
    
    df_final = df_indicators \
        .withColumnRenamed("Close", f"{ticker}_Close") \
        .drop("StdDev20", "Mean20")


    # Save in gold container each ticker as a CSV file

    gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{ticker}_indicators"

    (df_final
     .coalesce(1) 
     .write
     .mode("overwrite")
     .option("header", "true")
     .csv(gold_path))

    print(f"{ticker} saved successfully created in gold.")

print("Completed")