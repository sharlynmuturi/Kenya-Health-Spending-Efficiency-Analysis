# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG gov_spending;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

base_path = "/Volumes/gov_spending/bronze/raw_files/"

# COMMAND ----------

budget_df = spark.read.csv(
    base_path + "budgets/county_total_budget_allocation.csv",
    header=True,
    inferSchema=True
)

display(budget_df)

# COMMAND ----------

budget_breakdown_df = spark.read.csv(
    base_path + "budgets/county_budget_breakdown.csv",
    header=True,
    inferSchema=True
)

display(budget_breakdown_df)

# COMMAND ----------

population_df = spark.read.csv(
    base_path + "population/2019 Census Population Distribution by Sex, Number of Households, Land Area, Population Density and County (1).csv",
    header=True,
    inferSchema=True
)

display(population_df)

# COMMAND ----------

mortality_df = spark.read.csv(
    base_path + "outcomes/Mortality by County, 2019.csv",
    header=True,
    inferSchema=True
)

display(mortality_df)

# COMMAND ----------

maternal_df = spark.read.csv(
    base_path + "outcomes/Hospital Foetal, Neonatal and Maternal Deaths by County, 2016 -2020.csv",
    header=True,
    inferSchema=True
)

display(maternal_df)

# COMMAND ----------

facilities_df = spark.read.csv(
    base_path + "facilities/Number of Operational Health Facilities by Level, Type and Ownership.csv",
    header=True,
    inferSchema=True
)

display(facilities_df)

# COMMAND ----------

professionals_df = spark.read.csv(
    base_path + "professionals/Registered Health Professionals by Cadre.csv",
    header=True,
    inferSchema=True
)

display(professionals_df)

# COMMAND ----------

national_health_df = spark.read.csv(
    base_path + "expenditure/National Government Expenditure on Health Services.csv",
    header=True,
    inferSchema=True
)

display(national_health_df)

# COMMAND ----------

budget_df.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.budget")

# COMMAND ----------

spark.table("gov_spending.bronze.budget").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.budget").printSchema()

# COMMAND ----------

import re

def clean_column_names(df):
    new_cols = []
    for col in df.columns:
        col_clean = col.lower()  # lowercase
        col_clean = re.sub(r"[ ,;{}()\n\t=]", "_", col_clean)  # replace invalid chars
        col_clean = re.sub(r"_+", "_", col_clean)  # remove duplicate underscores
        col_clean = col_clean.strip("_")  # trim edges
        new_cols.append(col_clean)
    
    return df.toDF(*new_cols)

# COMMAND ----------

budget_breakdown_df_clean = clean_column_names(budget_breakdown_df)

display(budget_breakdown_df_clean)

# COMMAND ----------

budget_breakdown_df_clean.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.budget_breakdown")

# COMMAND ----------

spark.table("gov_spending.bronze.budget_breakdown").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.budget_breakdown").printSchema()

# COMMAND ----------

population_df_clean = clean_column_names(population_df)

display(population_df_clean)

# COMMAND ----------

population_df_clean.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.population")

# COMMAND ----------

spark.table("gov_spending.bronze.population").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.population").printSchema()

# COMMAND ----------

mortality_df.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.mortality")

# COMMAND ----------

spark.table("gov_spending.bronze.mortality").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.mortality").printSchema()

# COMMAND ----------

maternal_df_clean = clean_column_names(maternal_df)

display(maternal_df_clean)

# COMMAND ----------

maternal_df_clean.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.maternal_outcomes")

# COMMAND ----------

spark.table("gov_spending.bronze.maternal_outcomes").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.maternal_outcomes").printSchema()

# COMMAND ----------

facilities_df_clean = clean_column_names(facilities_df)

display(facilities_df_clean)

# COMMAND ----------

facilities_df_clean.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.facilities")

# COMMAND ----------

spark.table("gov_spending.bronze.facilities").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.facilities").printSchema()

# COMMAND ----------

professionals_df_clean = clean_column_names(professionals_df)

display(professionals_df_clean)

# COMMAND ----------

professionals_df_clean.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.professionals")

# COMMAND ----------

spark.table("gov_spending.bronze.professionals").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.professionals").printSchema()

# COMMAND ----------

national_health_df_clean = clean_column_names(national_health_df)

display(national_health_df_clean)

# COMMAND ----------

national_health_df_clean.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.bronze.national_health_expenditure")

# COMMAND ----------

spark.table("gov_spending.bronze.national_health_expenditure").show(5)

# COMMAND ----------

spark.table("gov_spending.bronze.national_health_expenditure").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gov_spending.bronze;

# COMMAND ----------

spark.table("gov_spending.bronze.population") \
    .select("administrative_unit") \
    .distinct() \
    .count()

# COMMAND ----------

