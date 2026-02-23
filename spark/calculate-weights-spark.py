import os

# Point this to your Java 17 installation path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("CalculateWeightsMapReduce").getOrCreate()

# Get input file from command line (defaulting to dataset2 if none provided)
input_file = sys.argv[1] if len(sys.argv) > 1 else "../dataset2-final.csv"

# 2. Define Schema (No headers in CSV)
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

# 3. Load Data
df = spark.read.csv(input_file, schema=schema,
    header=False,          # We know final.csv has no headers
    multiLine=True,        # Prevents newlines in reviews from shifting columns
    quote='"',             # Keeps commas inside reviews from breaking columns
    escape='"',
    #mode="DROPMALFORMED"   # Safely ignores completely broken rows)
)

# 4. Define Categories
categories = {
    "work_life_balance": r"work life balance|flexibility|flexible work|flexible hours",
    "culture_values": r"culture|values|ethic|environment",
    "diversity_inclusion": r"diversity|inclusion|equality",
    "career_opp": r"progression|career|training|promotion",
    "comp_benefits": r"salary|pay|benefits|compensation",
    "senior_mgmt": r"senior management|management|manager|leadership",
}

# 5. Distributed Count (MapReduce style)
for category, pattern in categories.items():
    # .rlike is PySpark's regex function
    pros_count = df.filter(F.col("pros").rlike(pattern)).count()
    cons_count = df.filter(F.col("cons").rlike(pattern)).count()
    
    final_score = pros_count - cons_count
    print(f"{category}: {final_score}")

spark.stop()