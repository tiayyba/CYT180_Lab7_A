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

**Tip**: If Spark is not yet initialized in your Colab, run the same install steps from Lab 6 (Java 11, Spark 3.5.x, findspark) and then create the `SparkSession`.

----

## Section 1 — Simple DataFrames & Basic Joins

In this section, you will create two small DataFrames and learn how Spark performs different types of joins.  
Joins are one of the most important operations in data engineering because real-world datasets are almost never stored in a single table.

We will use two tables:

- `df_people` – information about individuals  
- `df_salary` – salary records keyed by `person_id`

Let's create these tables.

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


### 2. What is an nner Join?

An **inner join** returns only the rows where the join key exists *in both* DataFrames.

- If a `person_id` appears in both tables → it is included  
- If it appears in only one → it is excluded

Think of it as: **"Show me only matching rows."**
In PySpark, you specify the join condition using the `on=` parameter in `DataFrame.join()`.

The following query will show only the people who have a matching salary record. It excludes anyone without a salary and excludes any salary entries that don’t have a matching person.

```python
df_inner = df_people.join(df_salary, on="person_id", how="inner")
df_inner.show()
```

You can also provide a boolean expression:  
```python
df_left = df_people.join(df_salary, df_people.person_id == df_salary.person_id, "left")
```
In both cases, the **join key** is defined **with `on`** (either as a column name or a boolean expression). The `how` argument controls the join type: `"inner"` etc.


###  What is a Left Join?

A **left join** keeps *all* rows from the left DataFrame (`df_people`), and adds matching data from the right DataFrame (`df_salary`) when available.

- If the left row has no match → Spark fills missing values with `null`
- Nothing is removed from the left table

Think of it as:  
**"Keep everyone in the people table; add salary data if available."**

Let's write a query to: "
List everyone from the people table, and include salary if available. If a person has no salary record, show null for salary."

```python
df_left = df_people.join(df_salary, on="person_id", how="left")
df_left.show()
```


###  What is an **Anti Join**?

An **anti join** returns rows from one DataFrame where **no match exists** in the other.

Specifically:

- A **right anti join** shows rows in the right table (`df_salary`)  
  that *do not* have a matching `person_id` in the left table (`df_people`)

This is extremely useful for:

- finding data quality issues  
- detecting orphan records  
- identifying missing relationships  

Think of it as:  
**"Show me the records that failed to match."**


### Student Task 1 — Anti Join
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


Real-world data pipelines almost always combine:

1. **Fact data** – large tables with daily records (e.g., COVID cases per day)  
2. **Dimension data** – small tables with descriptive attributes (e.g., population, region, categories)

Spark joins allow us to merge these two types of data into a single enriched dataset.

In this section, you will create a small lookup table containing population and continent information for the five countries used earlier.  
You will later join this with COVID‑19 data to compute per‑capita metrics such as `cases_per_million`.

### Create Population/Continent Data

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
```
----

## Section 3 — Load COVID Dataset (Same Source as Lab 6)

In this section, you will **download and load** the Our World in Data (OWID) COVID‑19 dataset into a Spark DataFrame and prepare it for joining by ensuring the `date` column has the proper Spark `date` type.  
We will then **filter to five focus countries** so downstream tasks are fast and easy to inspect in Colab.

- Large “fact” datasets like daily COVID records are common in analytics.
- Proper data types (e.g., `date`) are important for ordering, window functions, and time‑based joins/aggregations.
- Filtering to a small, focused subset is a common step to accelerate iteration in development.

### 1. Download & Load the CSV

The cell below downloads the latest CSV into `/content` (Colab’s working directory) if it’s not already present, and loads it with `inferSchema=True` so Spark can detect numeric types.  
We then cast the `date` string into a proper Spark `date` type so that time operations will work as expected.

```python
import requests, pathlib
from pyspark.sql import functions as F

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
local_path = pathlib.Path("/content/owid-covid-data.csv")

# Download the file if it doesn't exist yet
if not local_path.exists():
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    local_path.write_bytes(r.content)

# Load with header + schema inference
df = spark.read.csv(str(local_path), header=True, inferSchema=True)

# Convert 'date' to a proper Spark DateType
df = df.withColumn("date", F.to_date("date"))

print("Row count (may take a few seconds):", df.count())
df.printSchema()
