# Databricks notebook source
df = spark.table("gov_spending.gold.county_efficiency")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EFFICIENCY RANKING
# MAGIC `Which counties are most/least efficient?`

# COMMAND ----------

display(
    df.select("County", "efficiency_score")
      .orderBy("efficiency_score", ascending=False)
      .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Identify top performers
# MAGIC - Shows efficiency gaps across counties

# COMMAND ----------

display(
    df.select("County", "efficiency_score")
      .orderBy("efficiency_score")
      .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC - These are the policy priority counties

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPENDING VS OUTCOMES
# MAGIC
# MAGIC `Does more spending improve outcomes?`

# COMMAND ----------

display(
    df.select("health_spend_per_capita", "outcome_score", "County")
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Efficient counties - top-left (low spend, high outcome)
# MAGIC - Inefficient - bottom-right (high spend, low outcome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLUSTER VISUALIZATION

# COMMAND ----------

display(
    df.select(
        "health_spend_per_capita",
        "outcome_score",
        "prediction",
        "County"
    )
)

# COMMAND ----------

display(
    df.groupBy("prediction").count()
)

# COMMAND ----------

display(
    df.select("prediction", "County", "efficiency_score")
      .orderBy("prediction", "efficiency_score", ascending=False)
)

# COMMAND ----------

display(
    df.groupBy("prediction")
      .avg(
          "health_spend_per_capita",
          "outcome_score",
          "efficiency_score"
      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HEALTH BUDGET SHARE ANALYSIS
# MAGIC
# MAGIC `Do counties prioritizing health perform better?`

# COMMAND ----------

display(
    df.select("health_share", "outcome_score", "County")
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Weak correlation - inefficiency in allocation
# MAGIC - Strong correlation - funding matters

# COMMAND ----------

# MAGIC %md
# MAGIC ## MORTALITY BREAKDOWN
# MAGIC `Which mortality factor drives poor outcomes?`

# COMMAND ----------

display(
    df.select(
        "County",
        "infant_mortality_rate",
        "maternal_mortality_rate",
        "under_5_mortality_rate"
    ).limit(10)
)

# COMMAND ----------

mortality_long = df.select(
    "County",
    expr("stack(3, \
        'Infant', infant_mortality_rate, \
        'Maternal', maternal_mortality_rate / 10, \
        'Under-5', under_5_mortality_rate) as (Mortality_Type, Rate)")
)

display(mortality_long)

# COMMAND ----------

# MAGIC %md
# MAGIC This is a diagnostic chart:
# MAGIC
# MAGIC - High maternal mortality - poor maternal care systems
# MAGIC - High infant mortality - neonatal care gaps
# MAGIC - High under-5 - broader system issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPENDING DISTRIBUTION
# MAGIC
# MAGIC `How unequal is spending across counties?`

# COMMAND ----------

display(
    df.select("health_spend_per_capita")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EFFICIENCY SCORE DISTRIBUTION
# MAGIC `Are most counties average or extreme?`

# COMMAND ----------

display(
    df.select("efficiency_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPENDING COMPOSITION (RECURRING VS DEVELOPMENT)
# MAGIC `Does spending structure affect efficiency?`

# COMMAND ----------

display(
    df.select(
        "County",
        "Health_Recurrent",
        "Health_Development"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Over-reliance on recurrent vs development spending

# COMMAND ----------

from pyspark.sql.functions import expr

spending_long = df.select(
    "County",
    expr("stack(2, \
        'Recurrent', Health_Recurrent, \
        'Development', Health_Development) as (Spending_Type, Amount)")
)

display(spending_long)

# COMMAND ----------

# MAGIC %md
# MAGIC - Shows how counties allocate health budgets
# MAGIC
# MAGIC - Some counties heavily skew toward recurrent (salaries)
# MAGIC Others invest more in development (infrastructure)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RANK VS SPENDING
# MAGIC
# MAGIC `Does higher spending leads to better rank`

# COMMAND ----------

display(
    df.select("efficiency_rank", "health_spend_per_capita")
)

# COMMAND ----------

