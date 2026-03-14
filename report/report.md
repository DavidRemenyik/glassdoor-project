# Fat Data Dudes: Full Project Documentation
**Course:** COMP30770 Programming for Big Data  
**Project:** Multi-Dimensional Weighted Sentiment Analysis of 10.7M Glassdoor Reviews

---

## 1. Introduction & Project Identity
### Team & Goal
Our team, **Fat Data Dudes**, set out to move beyond simple "star ratings." We wanted to define a company's "true value" by analyzing what employees actually say in their written reviews.
*   **Objective:** Process 10,740,378 reviews to identify the top-tier companies globally using a custom weighted sentiment model.
*   **Dataset:** Two combined Kaggle datasets totaling ~1.5 GB in CSV format.
*   **Big Data Justification:** 
    *   **Volume:** 10.7M records exceed the memory capacity of standard Python/Pandas applications.
    *   **Variety:** Mixed-mode data (Floating point ratings, categorical firms/locations, and unstructured "Pros/Cons" text).
    *   **Velocity:** Real-world Glassdoor data grows rapidly; our pipeline is designed to be "iterative" (as taught in Lecture 5).
*   **Members:** 
    *   **David Remenyik**
    *   **Felim Sullivan**
    *   **Adam Kearns**
    *   **Ben Rattray**
    *   **Darragh Bulter**

---

## 2. Stage 1: Data Pre-processing & Cleaning
Before any analysis, we had to solve the "Comma Problem." Because employee "Pros" and "Cons" often contain commas, a standard `split(',')` would break the schema.
1.  **Ingestion:** Data was downloaded via Kaggle API and unzipped.
2.  **Cleaning Pipeline:** We developed `python/clean-commas.py` to handle quoted strings and escape sequences.
3.  **Name Extraction:** Using `python/company_name_extractor.py`, we normalized firm names since one dataset used a link the company reviews, and the other simply the company name (eg. Amazon and Reviews/Amazon-Reviews-E5462645.htm) to ensure accurate results.
4.  **Column Reordering:** We used `python/reorder_columns.py` to ensure the schema was consistent across both datasets before merging into `mergedFinal.csv`.

---

## 3. Stage 2: Feature Engineering (The Weighting Algorithm)
We didn't just pick weights at random. We calculated them based on the **entire 10.7M row corpus**.
*   **The Algorithm:** For every review, we scanned the "Pros" and "Cons" for specific keywords (e.g., "overtime," "toxic," "benefits," "flexible").
*   **Calculation:** 
    *   `Score = (Mentions in Pros) - (Mentions in Cons)`
*   **The Discovery:** We found that "Culture Values" had a massive total score (~1.1M mentions), while "Senior Management" had a negative score (~ -883k), indicating it is more often cited as a "Con."
*   **Final Weights:** 
    *   Work-Life Balance: **0.1197**
    *   Culture Values: **0.4247**
    *   Career Opps: **-0.0129**
    *   Comp & Benefits: **0.2584**
    *   Senior Mgmt: **-0.3348**

---

## 4. Stage 3: Traditional Solution (MySQL)
To establish a baseline (as required by Lab 2), we implemented a SQL solution.
*   **Setup:** Used `docker-compose` to spin up a MySQL 8.0 container.
*   **Ingestion:** Used `LOAD DATA LOCAL INFILE` for high-speed insertion.
*   **Baseline Query:** A single-threaded SQL query calculated the weighted average across 10.7M rows.
*   **Results:**
    *   **Execution Time:** 1 minute 51 seconds.
    *   **Bottleneck:** High **Disk I/O**. As taught in Lecture 5, traditional DBs suffer from the "2 writes + 2 reads" cycle for every intermediate step.

---

## 5. Stage 4: MapReduce Optimisation (Spark RDD)
The core of the project was migrating to Apache Spark to utilize **In-Memory Cluster Computing**.

### Why RDD Core API?
Per the project brief, we utilized the **Spark Core API (RDD)** rather than DataFrames. This allowed us to explicitly define the Map and Reduce phases:
1.  **Fault Tolerance:** We utilized RDD **Lineage**. If a node fails while processing the 1.5GB file, Spark uses the lineage graph to recompute only the missing partition.
2.  **The "Serialization Tax":** We discovered that moving 10.7M Python objects to the JVM is slow. To optimize this, we implemented:
    *   **`mapPartitions`**: Instead of calling a function 10.7 million times, we call it once per partition (~100 times), passing a batch of rows.
    *   **KryoSerializer**: Replaced the default Java serializer for faster shuffle performance.
    *   **`csv.reader`**: Used the Python `csv` module inside the Map phase for robust parsing of text fields.

### Native Performance (Scala RDD)
To further investigate the "Serialization Tax," we implemented a native **Scala RDD** version (`spark/GlassdoorAnalysis.scala`). 
*   **Startup Overhead:** Because we used the `spark-shell` to run the Scala code, the initialization of the JVM and Spark environment adds significant overhead (~35-40 seconds).
*   **Processing Speed:** Once initialized, the Scala implementation processed the 10.7M rows in just **24.8 seconds**. 
*   **Advantage:** By running directly on the JVM, we completely bypassed the Python-to-Java communication layer (Py4J). This version achieved the highest throughput for raw RDD processing on CSV data.

### MapReduce Steps:
*   **Map:** `parse_partition` -> Emits `(FirmName, (1, WeightedScore))`
*   **Shuffle:** Spark groups all reviews for the same firm across the network.
*   **Reduce:** `reduceByKey` -> Aggregates `(sum_count, sum_score)`.
*   **Filter:** `filter(count >= 1000)` -> Removes statistical noise.

---

## 6. Stage 5: Beyond the Paradigm (DataFrames & Parquet)
As an extension of our research, we explored more modern Spark features to see how far we could push the performance.

### DataFrames vs. RDDs
We implemented the same logic using **Spark DataFrames** (`sql-query-spark.py`). DataFrames use the **Catalyst Optimizer** to generate an efficient execution plan.
*   **DataFrame (CSV):** While faster than MySQL, it still suffered from parsing overhead, taking **1 minute 41 seconds**.

### The Parquet Optimization
We converted our 1.5GB CSV into **Apache Parquet**, a columnar storage format. 
*   **DataFrame (Parquet):** The execution time dropped to a staggering **16 seconds**. This is because Parquet only reads the specific columns needed for our formula (WLB, Culture, etc.), skipping the heavy text of "Pros" and "Cons."
*   **RDD (Parquet):** We attempted to read Parquet directly into an RDD (`rdd-parquet-spark.py`). This was actually **slower (2m 10s)** than the CSV-based RDD. This taught us that the RDD Core API is not optimized for complex columnar formats; the conversion from Parquet's structured schema back to a flat Python RDD creates more overhead than simply reading a raw text file.

---

## 7. Final Results & Comparison
| Technology | Storage | Time (s) | Throughput (Rows/s) | Core Utilization |
| :--- | :--- | :--- | :--- | :--- |
| **Traditional (MySQL)** | Disk (SQL) | 111s | ~96,000 | 1 Core (25%) |
| **Spark RDD (Standard)** | Disk (CSV) | 55s | ~195,000 | 4 Cores (100%) |
| **Spark RDD (Optimized)** | Disk (CSV) | 42s | ~255,000 | 4 Cores (100%) |
| **Spark RDD (Scala)** | Disk (CSV) | **~25s*** | **~430,000** | 4 Cores (100%) |
| **Spark DataFrame** | Disk (CSV) | 101s | ~106,000 | 4 Cores (100%) |
| **Spark RDD** | **Parquet** | 130s | ~82,000 | 4 Cores (100%) |
| **Spark DataFrame** | **Parquet** | **16s** | **~671,000** | 4 Cores (100%) |

*\*Processing time only. Startup overhead via `spark-shell` adds ~35s.*

### Final Discovery Conclusion
Our journey through the Big Data stack confirms that while **MapReduce (RDD)** provides the most control and transparency for custom algorithms, the choice of **Language** and **Storage Format** are the two biggest performance levers:
1.  **Language:** Moving from Python to native **Scala** halved the RDD processing time from 55s to 25s by eliminating the Py4J serialization tax.
2.  **Storage:** Converting to **Apache Parquet** dropped the DataFrame time to a staggering **16 seconds** by leveraging columnar storage.

For the purposes of the foundational MapReduce paradigm as taught in COMP30770, our **Optimized Scala RDD (25s)** represents the most efficient use of the core API on raw text data, proving that JVM-native execution is critical for high-performance Big Data pipelines.

---
**Technical Stack:** Docker, MySQL, Apache Spark (RDD & DataFrame APIs), Apache Parquet, Python 3.10, OpenJDK 17.
