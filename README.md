# Government Spending Efficiency Analyzer

### Evaluating Health Expenditure Efficiency Across Kenyan Counties


## 1. Project Overview


This project analyzes how efficiently Kenyan counties utilize public health funds to produce measurable healthcare outcomes.

It answers a critical policy question:

> **Are counties using public funds efficiently to improve health outcomes?**

Using a combination of **data engineering (PySpark + Medallion Architecture)** and **analytical modeling**, the project quantifies efficiency, ranks counties, and generates actionable policy insights.


## 2. Objectives


*   Measure **efficiency of health spending** at the county level
*   Compare **resource allocation vs actual outcomes**
*   Identify **high-performing and underperforming counties**
*   Provide **data-driven policy recommendations**
*   Build a **scalable analytics pipeline** using modern data architecture
    

## 3. Project Architecture (Medallion Framework)

The project follows a **Medallion Architecture** to ensure scalability, reproducibility, and clarity.

### Bronze Layer (Raw Data)

*   Ingested raw CSV datasets into Delta tables
*   No transformations applied
*   Preserves original data for traceability
    

**Datasets include:**

*   County budget allocations
*   Budget breakdown (recurrent vs development)
*   Population (2019 Census)
*   Mortality indicators (2019)
*   Maternal, neonatal, and foetal deaths (2016–2020)
*   Health facilities (national level)
*   Health professionals (national level)
*   National health expenditure trends
    

### Silver Layer (Clean & Structured Data)

This layer transforms raw data into **analysis-ready datasets**.

#### Key transformations:

*   Standardized county names across all datasets
*   Cleaned and normalized column formats
*   Filtered population data to extract total population
*   Pivoted mortality indicators into structured columns
*   Averaged multi-year datasets (2018–2020) for stability
*   Created derived metrics such as:
    *   Health spending share
    *   Population-adjusted indicators
        

#### Output:

A unified dataset:

    master_county_health

This table contains:

*   Budget metrics
*   Population data
*   Mortality indicators
*   Derived features (e.g., health_spend_per_capita)


### Gold Layer (Analytics & Modeling)

This is the **core analytical layer** where efficiency is calculated.

## 4. Efficiency Model

### Model Approach: Interpretable Composite Index

The model evaluates how well counties convert **inputs (spending)** into **outputs (health outcomes)**.

### Inputs (Cost)

*   Health spending per capita
    

### Outputs (Outcomes)

*   Infant mortality rate
*   Maternal mortality rate
*   Under-5 mortality rate
    

### Methodology

#### Step 1: Normalization

All variables are scaled using **Min-Max normalization** to ensure comparability.

#### Step 2: Outcome Scoring

Mortality indicators are inverted so that:

*   Lower mortality - Higher score
    
A composite **Outcome Score** is computed as:

    Average of normalized outcome indicators

#### Step 3: Cost Efficiency Calculation

Efficiency is defined as:

    Efficiency = Outcome Score / Normalized Spending

This captures:

> “How much outcome improvement is achieved per unit of spending”

#### Step 4: Final Efficiency Score (0–100)

Scores are rescaled to a **0–100 range** for interpretability.

## 5. Key Outputs

The final dataset:

    gov_spending.gold.county_efficiency

### Contains:

## | Column | Description |
| --- | --- |
| County | County name |
| health_spend_per_capita | Spending adjusted for population |
| outcome_score | Composite health outcome score |
| efficiency_score | Final efficiency metric (0–100) |
| efficiency_rank | Ranking of counties |
| prediction | Cluster group |


## 6. Analytical Outputs

### 1. Efficiency Rankings

*   Identifies **top-performing counties**
*   Highlights **inefficient counties**
    

### 2. Spending vs Outcomes Analysis

*   Reveals weak correlation between spending and results
*   Demonstrates **diminishing returns on expenditure**
    

### 3. Clustering (K-Means)

Counties are grouped into 4 clusters:

| Cluster | Interpretation |
| --- | --- |
| High spend + high outcome | Efficient |
| High spend + low outcome | Inefficient |
| Low spend + high outcome | Highly efficient |
| Low spend + low outcome | Under-resourced |


### 4. Key Insights

*   High spending does not guarantee better outcomes
*   Some counties achieve strong results with limited resources
*   Structural challenges affect performance in certain regions
*   Efficiency varies significantly across counties
    

## 7. Visualization (Planned / Implemented)

The dataset supports interactive dashboards in **Power BI / Tableau**, including:

*   Choropleth map (efficiency by county)
*   Spending vs outcome scatter plots
*   Efficiency ranking bar charts
*   Cluster segmentation visuals
    

## 8. Policy Recommendations

Based on the analysis:

### Resource Optimization

*   Reallocate funds toward high-impact interventions
    

### Benchmarking

*   Adopt best practices from efficient counties
    

### System Strengthening

*   Improve healthcare delivery systems in underperforming regions
    

### Strategic Planning

*   Focus on **efficiency**, not just spending levels
    

## 9. Limitations

*   Lack of county-level data on:

    *   Health facilities
    *   Health workforce
        
*   Some datasets required averaging across years
*   Mortality data may not fully capture healthcare access
    


## 10. Future Improvements

*   Incorporate **facility and workforce data at county level**
*   Add **time-series analysis (multi-year trends)**
*   Build **predictive models (e.g., efficiency forecasting)**
    

## 11. Tech Stack

*   **PySpark (Databricks)** - Data processing
*   **Delta Lake** - Data storage
*   **Unity Catalog** - Data governance
*   **Power BI / Tableau** - Visualization
*   **Python** - Feature engineering
    

## 12. Conclusion

This project demonstrates that:

> **Efficiency in public health spending is driven not just by how much is spent, but how effectively resources are utilized.**

By combining data engineering and analytics, the project provides a **framework for evaluating public sector performance** and supports **evidence-based decision-making**.