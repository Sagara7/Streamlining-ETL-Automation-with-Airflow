# Streamlining ETL Automation with Airflow

Made by : [Sagara Biru Wilantara](https://www.linkedin.com/in/sagara-biru)


# Table of Contents

1. [Introduction](#introduction)
2. [Project Objective](#project-objective)
3. [Data Source](#data-source)
4. [Technologies and Libraries Used](#technologies-and-libraries-used)
5. [ETL Automation Workflow](#etl-automation-workflow)
6. [Data Transformation](#data-transformation)
7. [Data Loading](#data-loading)

---

## Introduction

This project focuses on streamlining ETL (Extract, Transform, Load) automation using Apache Airflow. The ETL process is designed to automate the transformation and loading of data from a PostgreSQL database to Elasticsearch.

---

## Project Objective

The main objective is to automate the ETL process for a dataset that focuses on Loan Prediction for a Company.

---

## Data Source

The cleaned data used in this project is stored in `Data_Clean.csv`, which contains various features related to loan prediction.

---

## Technologies and Libraries Used

- Python
- Apache Airflow
- pandas
- PostgreSQL
- Elasticsearch

---

## ETL Automation Workflow

The ETL process is orchestrated using Apache Airflow, with the DAG defined in `Sagara_Biru_DAG.py`.

---

## Data Transformation

Data transformations are executed in Python, with the help of the pandas library, as outlined in `Sagara_Biru_GX.py`.

---

## Data Loading

The transformed data is loaded into Elasticsearch for further analysis and visualization.

---
