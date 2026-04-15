import tabula
import pandas as pd

pdf_path = "county_total_budget_allocation.pdf"

df1_list = tabula.read_pdf(
    pdf_path,
    pages="1-2",
    multiple_tables=True,
    stream=True,
    lattice=False,
    pandas_options={"header": None}
)

df1_raw = pd.concat(df1_list, ignore_index=True)

# fix column names manually
df1_raw.columns = [
    "County",
    "Total_Budget",
    "Health_Total",
    "Health_Recurrent",
    "Health_Development"
]


# remove header rows
df1_raw = df1_raw[
    ~df1_raw.astype(str).apply(
        lambda row: row.str.contains("County|Total Budget|Health Allocation", na=False).any(),
        axis=1
    )
]

# keep only real numeric rows
df1_raw = df1_raw[
    df1_raw["Total_Budget"].astype(str).str.replace(",", "").str.isnumeric()
].reset_index(drop=True)

print(df1_raw.head())

# Renaming for consistency
df1 = df1_raw.copy()

# If health allocation comes in a merged column, split it
# Expected structure:
# County | Total Budget | Health Allocation (Total, Recurrent, Development)

# Try handling merged columns safely
df1 = df1.dropna(how="all")

print("\nTABLE 1 RAW:\n", df1.head())


# Clean split - Health Allocation is a single column with multiple values:
if "Health Allocation" in df1.columns:
    health_split = df1["Health Allocation"].str.split(r"\s{2,}", expand=True)

    health_split.columns = [
        "Health_Total",
        "Health_Recurrent",
        "Health_Development"
    ]

    df1 = pd.concat([df1.drop(columns=["Health Allocation"]), health_split], axis=1)

print("\nCLEAN TABLE:\n", df1.head())

df1.to_csv("county_total_budget_allocation.csv", index=False)