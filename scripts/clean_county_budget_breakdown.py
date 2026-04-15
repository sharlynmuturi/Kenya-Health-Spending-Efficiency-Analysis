import tabula
import pandas as pd

pdf_path = "county_budget_breakdown.pdf"

df2_list = tabula.read_pdf(
    pdf_path,
    pages="1-2",
    multiple_tables=True,
    lattice=False,
    stream=True
)

df2_raw = pd.concat(df2_list, ignore_index=True)

df2_raw = df2_raw[
    ~df2_raw.astype(str).apply(
        lambda row: row.str.contains(
            "County|medical|emoluments maintenance|supplies",
            na=False
        ).any(),
        axis=1
    )
].reset_index(drop=True)

print(df2_raw.head())

df2_raw.columns = df2_raw.columns.str.strip()
df2 = df2_raw.copy()

df2 = df2.dropna(how="all")

print("\nTABLE 2 RAW:\n", df2.head())

# Expected structure:
# County | Recurrent Breakdown | Development Breakdown

# Split recurrent breakdown into components
if "Recurrent Breakdown" in df2.columns:
    rec_split = df2["Recurrent Breakdown"].str.split(r"\s{2,}", expand=True)

    rec_split.columns = [
        "Personnel_Emoluments",
        "Operations_Maintenance",
        "Drugs_Medical_Supplies",
        "Training_Expenses",
        "Other_Recurrent"
    ]

    df2 = pd.concat([df2.drop(columns=["Recurrent Breakdown"]), rec_split], axis=1)



if "Development Breakdown" in df2.columns:
    dev_split = df2["Development Breakdown"].str.split(r"\s{2,}", expand=True)

    dev_split.columns = [
        "Buildings",
        "Equipment_Furniture",
        "Grants_Transfers_Other"
    ]

    df2 = pd.concat([df2.drop(columns=["Development Breakdown"]), dev_split], axis=1)


print("\nCLEAN TABLE 2:\n", df2.head())

df2.to_csv("county_budget_breakdown.csv", index=False)