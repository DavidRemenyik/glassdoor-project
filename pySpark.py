from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, FloatType
from pyspark.sql.functions import col, count, avg
# Define the schema based on the employee table structure
schema = StructType([
    StructField("firm", StringType(), True),
    StructField("date_review", DateType(), True),
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

# To load your CSV using this schema:
df = spark.read.csv("../dataset2-final.csv", schema=schema, header=False)


# Assuming your data is loaded in

# THE MAP PHASE (Filtering and calculating the weighted score per row)
# We filter out nulls first, just like the SQL 'WHERE ... IS NOT NULL'
mapped_df = df.dropna(subset=['work_life_balance', 'culture_values', 'career_opp', 'comp_benefits', 'senior_mgmt']) \
    .withColumn("row_weighted_score", 
        (
            (col("work_life_balance") * 1.1197) + 
            (col("culture_values") * 1.4247) + 
            (col("career_opp") + (col("career_opp") * -0.0129)) + 
            (col("comp_benefits") * 1.2584) + 
            (col("senior_mgmt") + (col("senior_mgmt") * -0.3348))
        ) / 5
    )

# THE REDUCE PHASE (Grouping by firm, aggregating counts and averages)
reduced_df = mapped_df.groupBy("firm") \
    .agg(
        count("*").alias("review_count"),
        avg("row_weighted_score").alias("weighted_rating")
    )

# THE FILTER & SORT (Equivalent to HAVING and ORDER BY)
final_top_10 = reduced_df.filter(col("review_count") >= 1000) \
    .orderBy(col("weighted_rating").desc()) \
    .limit(10)

final_top_10.show()