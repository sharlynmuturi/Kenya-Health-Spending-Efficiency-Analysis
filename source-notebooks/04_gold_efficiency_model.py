# Databricks notebook source
# MAGIC %md
# MAGIC Goal is to create:
# MAGIC
# MAGIC - A clear efficiency score (0–100)
# MAGIC - County rankings
# MAGIC - Clusters (for insights)
# MAGIC - Actionable interpretation
# MAGIC
# MAGIC
# MAGIC ## Loading Silver Data

# COMMAND ----------

df = spark.table("gov_spending.silver.master_county_health")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model components
# MAGIC
# MAGIC #### Input (Cost - Lower is better (for efficiency))
# MAGIC `health_spend_per_capita`
# MAGIC
# MAGIC #### Outputs (Outcomes - Lower is better)
# MAGIC
# MAGIC `Infant mortality rate`,
# MAGIC `Maternal mortality rate`,
# MAGIC `Under-5 mortality rate`,
# MAGIC `Crude death rate`
# MAGIC
# MAGIC ## Normalizing Variables (0-1)
# MAGIC ### Applying Min-Max Scaling

# COMMAND ----------

from pyspark.sql.functions import min, max

def min_max_scale(df, col_name):
    stats = df.select(
        min(col_name).alias("min"),
        max(col_name).alias("max")
    ).collect()[0]
    
    min_val = stats["min"]
    max_val = stats["max"]
    
    return (df[col_name] - min_val) / (max_val - min_val)

# COMMAND ----------

from pyspark.sql.functions import col

df_scaled = df \
    .withColumn("spend_norm", min_max_scale(df, "health_spend_per_capita")) \
    .withColumn("infant_mortality_norm", min_max_scale(df, "infant_mortality_rate")) \
    .withColumn("maternal_mortality_norm", min_max_scale(df, "maternal_mortality_rate")) \
    .withColumn("under5_mortality_norm", min_max_scale(df, "under_5_mortality_rate"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inverting Mortality into Performance Score
# MAGIC After min-max scaling:
# MAGIC
# MAGIC | County | Infant Mortality Norm |
# MAGIC | --- | --- |
# MAGIC | A | 0.9 (bad) |
# MAGIC | B | 0.2 (good) |
# MAGIC
# MAGIC - Higher value = worse outcome
# MAGIC - But we want higher score = better health outcomes
# MAGIC
# MAGIC | Mortality Norm | Score |
# MAGIC | --- | --- |
# MAGIC | 0.9 (bad) | 0.1 |
# MAGIC | 0.2 (good) | 0.8 |

# COMMAND ----------

df_scaled = df_scaled \
    .withColumn("infant_score", 1 - col("infant_mortality_norm")) \
    .withColumn("maternal_score", 1 - col("maternal_mortality_norm")) \
    .withColumn("under5_score", 1 - col("under5_mortality_norm"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building Overall Outcome Score
# MAGIC
# MAGIC - Combining 3 indicators into one health performance index by computing mean (equal weighting)
# MAGIC
# MAGIC `“A county’s health outcome is the average of its performance in infant, maternal, and under-5 survival.”`

# COMMAND ----------

# Simple average

df_scaled = df_scaled.withColumn(
    "outcome_score",
    (
        col("infant_score") +
        col("maternal_score") +
        col("under5_score")
    ) / 3
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building Cost Efficiency Ratio
# MAGIC
# MAGIC This is the CORE efficiency idea:
# MAGIC
# MAGIC `“How much health outcome do we get per unit of spending? (Efficiency per unit cost)”`
# MAGIC
# MAGIC
# MAGIC | County | Outcome | Spend | Efficiency |
# MAGIC | --- | --- | --- | --- |
# MAGIC | A | 0.8 | 0.2 | HIGH |
# MAGIC | B | 0.8 | 0.8 | LOW |
# MAGIC
# MAGIC
# MAGIC
# MAGIC We want better outcomes with lower spending

# COMMAND ----------

df_scaled = df_scaled.withColumn(
    "cost_efficiency",
    col("outcome_score") / (col("spend_norm") + 0.01) # to avoid division issues
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Efficiency Score

# COMMAND ----------

from pyspark.sql.functions import min, max

# Finding lowest and highest efficiency in dataset
stats = df_scaled.select(
    min("cost_efficiency").alias("min"), 
    max("cost_efficiency").alias("max")
).collect()[0]

min_val = stats["min"]
max_val = stats["max"]

# Normalizing (converting raw efficiency into a 0–100 index)
df_final = df_scaled.withColumn(
    "efficiency_score",
    (col("cost_efficiency") - min_val) / (max_val - min_val) * 100
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ranking Counties

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Ranking rule (Sort counties from highest efficiency to lowest)
window = Window.orderBy(col("efficiency_score").desc())

df_final = df_final.withColumn(
    "efficiency_rank",
    row_number().over(window)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clustering Counties(For Insights)
# MAGIC
# MAGIC - Grouping counties into 4 intuitive categories.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handling nulls

# COMMAND ----------

from pyspark.sql.functions import col, count, when

df_final.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in ["health_spend_per_capita", "outcome_score"]
]).show()

# COMMAND ----------

df_final_clean = df_final.dropna(
    subset=["health_spend_per_capita", "outcome_score"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing features using VectorAssembler
# MAGIC
# MAGIC - ML algorithms (like KMeans) in PySpark do not accept separate columns directly but require one single column called features containing a vector of numbers.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["health_spend_per_capita", "outcome_score"],
    outputCol="features"
)

df_ml = assembler.transform(df_final_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running KMeans
# MAGIC
# MAGIC Telling the model:
# MAGIC
# MAGIC `“Group counties into 4 similar groups based on spending and outcomes.”`

# COMMAND ----------

from pyspark.ml.clustering import KMeans

kmeans = KMeans(k=4, seed=42)
model = kmeans.fit(df_ml)

# Assigning Clusters
df_final = model.transform(df_ml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpreting clusters
# MAGIC
# MAGIC `“What is the average behavior of each group?”`

# COMMAND ----------

display(
    df_final.groupBy("prediction")
    .avg("health_spend_per_capita", "outcome_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC | Cluster | Meaning |
# MAGIC | --- | --- |
# MAGIC | Moderate spend + high outcome | 3 - Efficient model |
# MAGIC | Low spend + acceptable outcome | 1 - Lean model |
# MAGIC | Moderate spend + poor outcome | 0 - Inefficient model |
# MAGIC | High spend + poor outcome | 2 - High-cost inefficient model |

# COMMAND ----------

df_final.write.mode("overwrite").format("delta") \
    .saveAsTable("gov_spending.gold.county_efficiency")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Visualizations
# MAGIC ### Top 10 Efficient Counties

# COMMAND ----------

display(
    df_final.orderBy(col("efficiency_score").desc()).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Least Efficient

# COMMAND ----------

display(
    df_final.orderBy(col("efficiency_score").asc()).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spend vs Outcome

# COMMAND ----------

display(
    df_final.select("health_spend_per_capita", "outcome_score")
)

# COMMAND ----------

display(
    df_final.select(
        "County",
        "Total_Budget",
        "population_2019",
        "health_spend_per_capita",
        "efficiency_score",
        "efficiency_rank",
        "outcome_score",
        "prediction"
    ).orderBy(col("health_spend_per_capita").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Insight: Health Spending Efficiency Across Kenyan Counties
# MAGIC
# MAGIC This table reveals a **strong disconnect between health spending levels and efficiency outcomes across counties** in Kenya.
# MAGIC
# MAGIC ### 1. High Spending Does Not Guarantee High Efficiency
# MAGIC Several counties with the **highest per capita health spending** do not rank highly in efficiency:
# MAGIC
# MAGIC - **Nairobi City** has the highest spending per capita (~1575) and the largest total health budget, yet ranks **1st in efficiency rank but only mid-level outcome score (0.60)**, indicating diminishing returns at very high spending levels.
# MAGIC - Counties like **Mombasa, Kiambu, and Uasin Gishu** also exhibit relatively high spending but only moderate efficiency scores.
# MAGIC
# MAGIC This suggests that beyond a certain threshold, **additional spending yields weaker marginal improvements in health outcomes**.
# MAGIC
# MAGIC ### 2. Low-Resource Counties Show Mixed Performance
# MAGIC Some lower-spending counties perform better than expected:
# MAGIC
# MAGIC - **Laikipia (rank 18)** and **Nyeri (rank 23)** achieve relatively strong outcome scores despite moderate spending levels.
# MAGIC - These counties demonstrate signs of **higher operational efficiency and better resource utilization**.
# MAGIC
# MAGIC This indicates that **management efficiency and service delivery quality matter as much as budget size**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Persistently Low Efficiency Counties
# MAGIC Several counties consistently underperform in both spending efficiency and outcomes:
# MAGIC
# MAGIC - **Turkana, Wajir, Mandera, West Pokot, and Garissa**
# MAGIC   - Moderate-to-low efficiency scores
# MAGIC   - Low outcome scores
# MAGIC   - High population pressure and geographic constraints
# MAGIC
# MAGIC These regions likely face **structural challenges (accessibility, infrastructure, workforce shortages)** rather than purely financial constraints.
# MAGIC
# MAGIC ### 4. Efficiency Clusters Are Clearly Visible
# MAGIC The clustering column (`prediction`) separates counties into clear groups:
# MAGIC
# MAGIC - **Cluster 1 (High efficiency / better outcomes)**:
# MAGIC   - Includes Nairobi, Kiambu, Uasin Gishu, Meru
# MAGIC   - Generally better outcomes and higher productivity per shilling spent
# MAGIC
# MAGIC - **Cluster 0 (Low efficiency despite high spending)**:
# MAGIC   - Includes Kilifi, Kakamega, Nakuru, Kisumu
# MAGIC   - Suggests inefficiencies in resource allocation or service delivery
# MAGIC
# MAGIC - **Cluster 3 (Low spending + moderate outcomes)**:
# MAGIC   - Includes Samburu, Marsabit, Embu, Baringo
# MAGIC   - Indicates constrained systems operating near capacity
# MAGIC
# MAGIC ### 5. Key Pattern: Diminishing Returns of Spending
# MAGIC A clear trend emerges:
# MAGIC
# MAGIC > Counties with extremely high health spending per capita do not always achieve proportional improvements in health outcomes.
# MAGIC
# MAGIC This supports the idea of **diminishing marginal returns in public health expenditure**.
# MAGIC
# MAGIC ### Policy Implications
# MAGIC
# MAGIC - **Reallocate resources toward efficiency gains, not just higher budgets**
# MAGIC - Strengthen **health system management in low-efficiency high-spend counties**
# MAGIC - Invest in **infrastructure and access in arid and semi-arid counties**
# MAGIC - Use efficient counties (e.g., Kiambu, Meru, Uasin Gishu) as **benchmark models**
# MAGIC - Shift focus from “how much is spent” to **“how effectively it is used”**
# MAGIC
# MAGIC ### Core Takeaway
# MAGIC
# MAGIC > “Kenya’s health system efficiency is not determined by spending levels alone, but by how effectively counties convert resources into improved health outcomes.”
# MAGIC
# MAGIC This analysis highlights **significant disparities in efficiency across counties**, providing a strong foundation for evidence-based policy decisions and resource reallocation.