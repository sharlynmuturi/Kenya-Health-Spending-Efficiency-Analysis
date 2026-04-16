# Government Spending Efficiency Analyzer

### Evaluating Health Expenditure Efficiency Across Kenyan Counties

## 1. Project Overview

This project analyzes how efficiently Kenyan counties convert public health spending into measurable healthcare outcomes.

It answers a key policy question:

> **Are counties using public funds efficiently to improve health outcomes?**

Using **PySpark, Databricks, and a Medallion Architecture**, the project builds a scalable data pipeline, constructs an interpretable efficiency model, and generates actionable insights for policy and planning.

_For a detailed analysis, methodology, and visual insights, see the full report included in this repository. `Report - Government Health Spending Efficiency Analysis.pdf`_

## 2. Objectives

*   Measure **efficiency of health spending** at the county level
*   Compare **resource allocation vs actual outcomes**
*   Identify **high-performing and underperforming counties**
*   Generate **data-driven policy recommendations**

## 3. Problem Statement

> **Which counties in Kenya are most efficient in converting health expenditure into improved healthcare outcomes?**

## 4. Key Insights

### 1. Spending Does Not Guarantee Efficiency

Higher spending does not consistently translate into better outcomes.  
Several high-spending counties achieve only moderate results, suggesting **diminishing returns** at higher budget levels.

### 2. Efficient Use of Limited Resources

Some counties deliver strong outcomes despite moderate or low spending, indicating **better resource utilization and operational efficiency**.

### 3. Structural Challenges in Low-Performing Counties

Counties such as Turkana, Wajir, and Mandera show persistently low efficiency, likely due to Geographic constraints, Limited infrastructure or workforce shortages

### 4. Clear Performance Segmentation

Clustering reveals distinct groups:

*   **Efficient:** Strong outcomes relative to spending
*   **Inefficient:** High spending, weak outcomes
*   **Under-resourced:** Low spending and poor outcomes
*   **High-performing:** High spending with strong outcomes

### 5. Core Insight

> **Efficiency is driven not by how much is spent, but by how effectively resources are used.**

## 5. Project Architecture (Medallion Framework)

The project follows a **Medallion Architecture** for scalability and reproducibility:

### Bronze Layer (Raw Data)

*   Ingested raw datasets into Delta tables
*   No transformations applied
*   Ensures traceability
    

**Datasets include:**

*   County budgets (total + breakdown)
*   Population (2019 Census)
*   Mortality indicators
*   Hospital Maternal, neonatal, and foetal deaths (2016–2020)
*   Health system data (facilities, workforce, expenditure)


> [Kenya Open Data Portal][https://kenya.opendataforafrica.org/]
> [The Kenya National Bureau of Statistics (KNBS)][https://www.knbs.or.ke/]
> [National and County Health Budget Analysis FY 2018/19][https://africacheck.org/sites/default/files/media/documents/2021-02/NATIONAL%20AND%20COUNTY%20HEALTH%20BUDGET%20ANALYSIS%20FY%202018-19.pdf]

### Silver Layer (Clean & Structured)

*   Standardized county names
*   Cleaned and normalized data
*   Pivoted and reshaped datasets
*   Aligned time periods (2018–2020 averages where needed)
*   Created derived metrics:
    *   Health spending per capita
    *   Health budget share
        

### Gold Layer (Analytics & Modeling)


*   Efficiency scoring model
*   County ranking
*   Clustering (K-Means segmentation)

## 6. Efficiency Model

### Approach: Simple, Interpretable Composite Index

The model evaluates how effectively counties convert **spending (input)** into **health outcomes (outputs)**.

- Input - Health spending per capita
- Outputs(Outcomes) - Infant mortality rate, Maternal mortality rate, Under-5 mortality rate
    

### Methodology

1.  **Normalization (Min-Max Scaling)**
2.  **Outcome Scoring**
    *   Mortality rates inverted (lower = better)
    *   Combined into a single **Outcome Score** 
3.  **Efficiency Calculation**
    Efficiency = Outcome Score / Normalized Spending
4.  **Final Score Scaling**
    *   Converted to a **0–100 efficiency score**


## 8. Analytical Outputs

### Efficiency Rankings

*   Identifies top and bottom performing counties
    

### Spending vs Outcomes

*   Shows weak correlation between spending and results
*   Highlights **diminishing returns**
    

### Clustering (K-Means)

# | Cluster | Interpretation |
| --- | --- |
| High spend + high outcome | Efficient |
| High spend + low outcome | Inefficient |
| Low spend + high outcome | Highly efficient |
| Low spend + low outcome | Under-resourced |


## 9. Visualizations

The project supports dashboards in **Databricks, Power BI, or Tableau**, including:

*   Efficiency ranking bar charts
*   Spending vs outcome scatter plots
*   Cluster segmentation analysis
*   Efficiency distribution (histogram)
*   Budget composition (recurrent vs development)
    
 _See the full report for integrated visual analysis and interpretations. `Report - Government Health Spending Efficiency Analysis.pdf`_

## 10. Policy Recommendations

### Resource Optimization

Reallocate funding toward high-impact interventions

### Benchmarking

Adopt best practices from efficient counties

### System Strengthening

Improve service delivery in underperforming regions

### Strategic Planning

Shift focus from **spending levels to spending efficiency**

## 11\. Limitations

*   Lack of county-level data on:
    *   Health facilities
    *   Health workforce
*   Some datasets required multi-year averaging
*   Mortality indicators may not fully capture healthcare access
    

## 12. Future Improvements

*   Integrate **facility and workforce data at county level**
*   Add **time-series analysis**
*   Develop **predictive models for efficiency trends**
