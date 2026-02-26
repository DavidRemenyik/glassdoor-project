from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

import os
import sys
# Point this to your Java 17 installation path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("SQLtoMapReduceQuery").getOrCreate()

input_file = sys.argv[1] if len(sys.argv) > 1 else "../dataset2-final.csv"

# 2. Define Schema
schema = StructType([
    StructField("firm", StringType(), True),
    StructField("date_review", StringType(), True), 
    StructField("job_title", StringType(), True),
    StructField("current", StringType(), True),
    StructField("location", StringType(), True),
    StructField("overall_rating", FloatType(), True),      # Matched to FLOAT
    StructField("work_life_balance", FloatType(), True),   # Changed to FloatType
    StructField("culture_values", FloatType(), True),      # Changed to FloatType
    StructField("career_opp", FloatType(), True),          # Changed to FloatType
    StructField("comp_benefits", FloatType(), True),       # Changed to FloatType
    StructField("senior_mgmt", FloatType(), True),         # Changed to FloatType
    StructField("recommend", StringType(), True),          # Changed to StringType to match VARCHAR(20)
    StructField("ceo_approv", StringType(), True),
    StructField("outlook", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("pros", StringType(), True),
    StructField("cons", StringType(), True)
])

# 3. Load Data (Make sure this points to your file)
df = spark.read.csv(input_file, schema=schema,
    header=False,          # We know final.csv has no headers
    multiLine=True,        # Prevents newlines in reviews from shifting columns
    quote='"',             # Keeps commas inside reviews from breaking columns
    escape='"',
    #mode="DROPMALFORMED"   # Safely ignores completely broken rows)
)

# 4. Filter out NULL values (The 'WHERE' equivalent)
filtered_df = df.filter(
    df.work_life_balance.isNotNull() &
    df.culture_values.isNotNull() &
    df.career_opp.isNotNull() &
    df.comp_benefits.isNotNull() &
    df.senior_mgmt.isNotNull()
)

# 5. GroupBy and Aggregation (The Map & Reduce equivalent)
result = filtered_df.groupBy("firm").agg(
    F.count("*").alias("review_count"),
    F.avg((
        (F.col("work_life_balance") * 1.1197) +
        (F.col("culture_values")    * 1.4247) +
        (F.col("career_opp")        * -0.0129 + F.col("career_opp")) +
        (F.col("comp_benefits")     * 1.2584) +
        (F.col("senior_mgmt")       * -0.3348 + F.col("senior_mgmt")))
    /5).alias("weighted_rating") 
)

# 6. Apply HAVING and ORDER BY logic
final_top_10 = result.filter(F.col("review_count") >= 1000) \
                     .orderBy(F.desc("weighted_rating")) \
                     .limit(10)

# 7. Show the final results
print("\n--- TOP 10 WEIGHTED COMPANIES ---")
final_top_10.show(truncate=False)

spark.stop()
