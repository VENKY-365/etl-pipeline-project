import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

print("=" * 50)
print("ETL PIPELINE STARTED")
print("=" * 50)

# -------------------------------------------------------
# EXTRACT — Read raw CSV data
# -------------------------------------------------------
print("\n[EXTRACT] Reading raw sales data...")
df = pd.read_csv("sales_data.csv")
print(f"Total records extracted: {len(df)}")
print(df.head())

# -------------------------------------------------------
# TRANSFORM — Clean and enrich the data
# -------------------------------------------------------
print("\n[TRANSFORM] Cleaning and transforming data...")

# 1. Keep only completed orders
df_cleaned = df[df["status"] == "completed"].copy()
print(f"Records after removing cancelled/pending: {len(df_cleaned)}")

# 2. Add a new column — Total Sale Amount
df_cleaned["total_amount"] = df_cleaned["quantity"] * df_cleaned["price"]

# 3. Convert order_date to proper date format
df_cleaned["order_date"] = pd.to_datetime(df_cleaned["order_date"])

# 4. Add a new column — Month
df_cleaned["order_month"] = df_cleaned["order_date"].dt.month_name()

# 5. Rename columns to be cleaner
df_cleaned.columns = [col.upper() for col in df_cleaned.columns]

print("Transformed Data:")
print(df_cleaned)

# -------------------------------------------------------
# LOAD — Save to CSV and SQLite Database
# -------------------------------------------------------
print("\n[LOAD] Loading data into output files...")

# Save to CSV
df_cleaned.to_csv("cleaned_sales_data.csv", index=False)
print("Saved to cleaned_sales_data.csv")

# Save to SQLite Database (like a mini data warehouse)
engine = create_engine("sqlite:///sales_warehouse.db")
df_cleaned.to_sql("sales_fact", engine, if_exists="replace", index=False)
print("Saved to sales_warehouse.db (SQLite Database)")

print("\n" + "=" * 50)
print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 50)
