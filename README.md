# ETL Pipeline Project

## Overview
An end-to-end ETL (Extract, Transform, Load) pipeline built with Python and SQL.

## What this project does
- **Extract** — Reads raw sales data from a CSV file
- **Transform** — Cleans data, filters completed orders, calculates total sales amount
- **Load** — Saves cleaned data to CSV and loads into a SQLite database (Data Warehouse)

## Technologies Used
- Python
- Pandas
- SQLAlchemy
- SQLite

## How to Run
1. Clone this repository
2. Install dependencies: `pip install pandas sqlalchemy`
3. Run the pipeline: `python etl_pipeline.py`

## Project Structure
- `sales_data.csv` — Raw input data
- `etl_pipeline.py` — Main ETL pipeline script
- `cleaned_sales_data.csv` — Transformed output data
- `sales_warehouse.db` — SQLite data warehouse
