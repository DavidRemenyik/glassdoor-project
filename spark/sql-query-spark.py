from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

import os

# Point this to your Java 17 installation path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("SQLtoMapReduceQuery").getOrCreate()

# 2. Define Schema
schema = StructType([
    StructField("firm", StringType(), True),
    StructField("date_review", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("current", StringType(), True),
    StructField("location", StringType(), True),
    StructField("overall_rating", IntegerType(), True),
    StructField("work_life_balance", IntegerType(), True),
    StructField("culture_values", IntegerType(), True),
    StructField("career_opp", IntegerType(), True),
    StructField("comp_benefits", IntegerType(), True),
    StructField("senior_mgmt", IntegerType(), True),
    StructField("recommend", FloatType(), True),
    StructField("ceo_approv", StringType(), True),
    StructField("outlook", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("pros", StringType(), True),
    StructField("cons", StringType(), True)
])

# 3. Load Data (Make sure this points to your file)
df = spark.read.csv("../dataset2-final.csv", schema=schema, header=False)

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
    F.avg(
        (F.col("work_life_balance") * 0.1197) +
        (F.col("culture_values")    * 0.4247) +
        (F.col("career_opp")        * -0.0129) +
        (F.col("comp_benefits")     * 0.2584) +
        (F.col("senior_mgmt")       * -0.3348)
    ).alias("weighted_rating")
)

# 6. Apply HAVING and ORDER BY logic
final_top_10 = result.filter(F.col("review_count") >= 1000) \
                     .orderBy(F.desc("weighted_rating")) \
                     .limit(10)

# 7. Show the final results
print("\n--- TOP 10 WEIGHTED COMPANIES ---")
final_top_10.show(truncate=False)

spark.stop()