# CYT180 — Lab 7: Joins, Data Cleaning, and Writing Data with PySpark

**Weight:** 3%  
**Work Type:** Individual  
**Submission Format:** Screenshots + short written answers  

---

##  Introduction

In Lab 6, you installed Spark in Google Colab, created DataFrames, performed common transformations, ran basic Spark SQL, and computed a 7‑day moving average. That workflow focused on working with **one dataset at a time**.

In real Big Data engineering, a common pattern is to **merge multiple datasets**, clean and standardize them, compute meaningful metrics, and **write the results** to efficient formats. This lab introduces three essential skills:

1. **Joining multiple DataFrames**
2. **Cleaning and deduplicating** real-world data
3. **Writing results** to disk using CSV and Parquet formats

You will mostly **run provided code** to observe Spark’s behavior and complete a few **simple tasks** in each section.

---

##  Learning Objectives

By the end of this lab, you will be able to:

- Perform inner, left, and anti joins in PySpark.
- Detect and handle missing values using `dropna()`, `fillna()`, and conditional logic.
- Remove duplicate rows using `dropDuplicates()`.
- Use Spark SQL to perform multi-table joins.
- Write DataFrames to disk using CSV and Parquet formats.
- Compare file sizes and explain why Parquet is preferred in big‑data systems.
- Explain how cleaning and joining datasets fit into a typical ETL (Extract‑Transform‑Load) workflow.

---

##  Prerequisites

- You have **Google Colab** access.
- You can initialize a **SparkSession** (from Lab 6).
- You can run cells and take **clear screenshots**.

> Tip: If Spark is not yet initialized in your Colab, run the same install steps from Lab 6 (Java 11, Spark 3.5.x, findspark) and then create the `SparkSession`.

---

# 🧪 LAB 7 — Spark Joins, Cleaning, and Writing Data

---

## Section 1 — Simple DataFrames & Basic Joins

In this section, you will run code that creates two small tables and performs different types of joins to illustrate the mechanics before applying them to real data.

### 1. Create Sample Data (Run)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

people = [
    (1, "John", "Smith", "Canada"),
    (2, "Jane", "Doe", "United States"),
    (3, "Ravi", "Kumar", "India"),
    (4, "Maria", "Silva", "Brazil")
]

salaries = [
    (1, 100000),
    (2, 150000),
    (5, 90000)   # no matching person_id in people
]

df_people = spark.createDataFrame(people, ["person_id","first","last","country"])
df_salary = spark.createDataFrame(salaries, ["person_id","salary"])

df_people.show()
df_salary.show()

```
### 1. Inner Join 

```python
df_inner = df_people.join(df_salary, on="person_id", how="inner")
df_inner.show()
```

### 1. Left Join 

```python
df_left = df_people.join(df_salary, on="person_id", how="left")
df_left.show()
```

### Student Task 1 — Anti Join (Write + Run)
Perform a right anti join on df_salary to find rows in the salary table that do not have a matching person in df_people.

Expected output includes person_id = 5.


Add your code below and include a screenshot of the result.

```python
# TODO: Right anti join on df_salary to find salaries with no matching people
# Your code here
```
### Reflection Question 1
In one sentence, explain why person_id = 5 appears in the anti join result.


----

## Section 2 — Load a Country Metadata Dataset

In this section, you will create a small lookup table containing population and continent information for the five countries used earlier.  
You will later join this with COVID‑19 data to compute per‑capita metrics.

### 1. Create Population/Continent Data (Run)

```python
data_pop = [
    ("Canada", 38000000, "North America"),
    ("United States", 331000000, "North America"),
    ("India", 1380000000, "Asia"),
    ("Brazil", 212000000, "South America"),
    ("Italy", 60000000, "Europe")
]

df_pop = spark.createDataFrame(data_pop, ["location","population","continent"])
df_pop.show()

----
