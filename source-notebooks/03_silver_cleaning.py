# Databricks notebook source
# MAGIC %md
# MAGIC ## Loading Bronze Tables

# COMMAND ----------

budget = spark.table("gov_spending.bronze.budget")
budget_breakdown = spark.table("gov_spending.bronze.budget_breakdown")
population = spark.table("gov_spending.bronze.population")
mortality = spark.table("gov_spending.bronze.mortality")
maternal = spark.table("gov_spending.bronze.maternal_outcomes")
facilities = spark.table("gov_spending.bronze.facilities")
professionals = spark.table("gov_spending.bronze.professionals")
national = spark.table("gov_spending.bronze.national_health_expenditure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardizing County Names

# COMMAND ----------

from pyspark.sql.functions import col, lower, trim, regexp_replace

def clean_county(col_name):
    return lower(trim(regexp_replace(col(col_name), " county", "")))

# COMMAND ----------

budget = budget.withColumn("county_clean", clean_county("county"))

population = population.withColumn("county_clean", clean_county("administrative_unit"))

mortality = mortality.withColumn("county_clean", clean_county("county"))

maternal = maternal.withColumn("county_clean", clean_county("administrative_unit"))

# COMMAND ----------

budget.select("county_clean").distinct().count()
population.select("county_clean").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Population Data
# MAGIC
# MAGIC ### Filtering total population only

# COMMAND ----------

population_clean = population.filter(
    (col("background_characteristic") == "Total Population") &
    (col("units") == "Number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting relevant columns

# COMMAND ----------

population_clean = population_clean.select(
    col("county_clean"),
    col("2019").alias("population_2019")
)

# COMMAND ----------

display(population_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Budget Data

# COMMAND ----------

budget_clean = budget.select(
    col("county_clean"),
    col("Total_Budget"),
    col("Health_Total"),
    col("Health_Recurrent"),
    col("Health_Development")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature engineering

# COMMAND ----------

from pyspark.sql.functions import col

budget_clean = budget_clean.withColumn(
    "health_share",
    col("Health_Total") / col("Total_Budget")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Mortality Data
# MAGIC
# MAGIC - Dataset is in long format - pivot required

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivoting indicators into columns

# COMMAND ----------

mortality_clean = mortality.groupBy("county_clean") \
    .pivot("Indicator") \
    .agg({"2019": "first"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns

# COMMAND ----------

mortality_clean = mortality_clean \
    .withColumnRenamed("Infant Mortality Rate (Per 1,000 Live Births)", "infant_mortality_rate") \
    .withColumnRenamed("Under 5 Mortality Rate (Per 1,000 Live Births)", "under_5_mortality_rate") \
    .withColumnRenamed("Crude Death Rate", "crude_death_rate") \
    .withColumnRenamed("Maternal Mortality Rate (Per 100,000 Live Births)", "maternal_mortality_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Hospital Foetal, Neonatal and Maternal Deaths by County Data
# MAGIC
# MAGIC - Using 2018–2020 average (more stable than single year)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting wide to usable format

# COMMAND ----------

from pyspark.sql.functions import expr

maternal_clean = maternal.withColumn(
    "avg_2018_2020",
    (col("2018") + col("2019") + col("2020")) / 3
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivoting indicators

# COMMAND ----------

maternal_clean = maternal_clean.groupBy("county_clean") \
    .pivot("indicator") \
    .agg({"avg_2018_2020": "first"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Facilities Data
# MAGIC The dataset contains national totals only, so to use as context variable
# MAGIC (or skip in county efficiency model)
# MAGIC
# MAGIC ### Creating national trend

# COMMAND ----------

facilities_clean = facilities.select(
    col("keph_level"),
    col("ownership"),
    col("2019")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Professional Data

# COMMAND ----------

professionals_clean = professionals.withColumn(
    "avg_2018_2020",
    (col("2018") + col("2019") + col("2020")) / 3
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA QUALITY CHECKS

# COMMAND ----------

budget.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

def to_double(column):
    return regexp_replace(col(column), ",", "").cast("double")

# COMMAND ----------

budget_clean = budget \
    .withColumn("Total_Budget", to_double("Total_Budget")) \
    .withColumn("Health_Total", to_double("Health_Total")) \
    .withColumn("Health_Recurrent", to_double("Health_Recurrent")) \
    .withColumn("Health_Development", to_double("Health_Development"))

# COMMAND ----------

budget_clean = budget_clean.withColumn(
    "health_share",
    col("Health_Total") / col("Total_Budget")
)

# COMMAND ----------

population_clean.printSchema()

# COMMAND ----------

population_clean = population_clean.withColumn(
    "population_2019",
    to_double("population_2019")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE TO SILVER
# MAGIC
# MAGIC - Joining Everything and ensuring clean column names

# COMMAND ----------

import re

def clean_columns(df):
    new_cols = []
    for col_name in df.columns:
        col_clean = col_name.lower()
        col_clean = re.sub(r"[ ,;{}()\n\t=]", "_", col_clean)
        col_clean = re.sub(r"_+", "_", col_clean)
        col_clean = col_clean.strip("_")
        new_cols.append(col_clean)
    return df.toDF(*new_cols)

# COMMAND ----------

mortality_clean = clean_columns(mortality_clean)

# COMMAND ----------

maternal_clean = clean_columns(maternal_clean)

# COMMAND ----------

master_df = budget_clean \
    .join(population_clean, "county_clean", "inner") \
    .join(mortality_clean, "county_clean", "left") \
    .join(maternal_clean, "county_clean", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature engineering (Core Metrics)
# MAGIC #### Spend per capita

# COMMAND ----------

master_df = master_df.withColumn(
    "health_spend_per_capita",
    col("Health_Total") / col("population_2019")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mortality per capita (if raw counts)

# COMMAND ----------

master_df = master_df.withColumn(
    "maternal_death_rate",
    col("maternal_deaths") / col("population_2019") * 100000
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing Values

# COMMAND ----------

from pyspark.sql.functions import count, when

master_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in master_df.columns
]).show()

# COMMAND ----------

print(master_df.columns)

# COMMAND ----------

display(master_df.describe())

# COMMAND ----------

master_df.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.silver.master_county_health")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Early Analysis
# MAGIC ### Spending vs Population

# COMMAND ----------

display(master_df.select("health_spend_per_capita"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top spending counties

# COMMAND ----------

display(
    master_df.orderBy(col("health_spend_per_capita").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mortality vs spending

# COMMAND ----------

display(
    master_df.select("health_spend_per_capita", "infant_mortality_rate")
)

# COMMAND ----------

