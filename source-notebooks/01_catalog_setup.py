# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS gov_spending;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gov_spending.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS gov_spending.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gov_spending.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN gov_spending;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS gov_spending.bronze.raw_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN gov_spending.bronze;

# COMMAND ----------

