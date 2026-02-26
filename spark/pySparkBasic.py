import os

# Point this to your Java 17 installation path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, FloatType

# 1. Initialize
spark = SparkSession.builder.appName("GlassdoorAnalysis").getOrCreate()

# 2. Define Schema (as we did before)
from pyspark.sql.types import StructType, StructField, StringType, FloatType

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

# 3. Load Data (Ensuring path is correct)
df = spark.read.csv("../dataset2-final.csv", schema=schema,
    header=False,          # We know final.csv has no headers
    multiLine=True,        # Prevents newlines in reviews from shifting columns
    quote='"',             # Keeps commas inside reviews from breaking columns
    escape='"',
    #mode="DROPMALFORMED"   # Safely ignores completely broken rows)
)
# 4. Simple test to see if it works
df.select("firm", "overall_rating").show(5)
