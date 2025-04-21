# DATA226 Lab 2 – Stock Price Pipeline with Airflow & dbt

This project implements an end-to-end ELT pipeline:
- DAG 1: Ingests historical stock data from yFinance into Snowflake
- DAG 2: Transforms raw data using dbt models (moving average, RSI)
- Output: Cleaned analytics tables ready for BI dashboards (Superset/Tableau)

## Technologies
- Apache Airflow (Docker)
- dbt (Snowflake adapter)
- yFinance
- Snowflake
