from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import os

# Point this to your Java 17 installation path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# 1. Initialize Spark
spark = SparkSession.builder.appName("ConvertData").getOrCreate()

# 2. Use your exact same schema from before
schema = StructType([
    StructField("firm", StringType(), True),
    StructField("date_review", StringType(), True), 
    StructField("job_title", StringType(), True),
    StructField("current", StringType(), True),
    StructField("location", StringType(), True),
    StructField("overall_rating", FloatType(), True),      
    StructField("work_life_balance", FloatType(), True),   
    StructField("culture_values", FloatType(), True),      
    StructField("career_opp", FloatType(), True),          
    StructField("comp_benefits", FloatType(), True),       
    StructField("senior_mgmt", FloatType(), True),         
    StructField("recommend", StringType(), True),          
    StructField("ceo_approv", StringType(), True),
    StructField("outlook", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("pros", StringType(), True),
    StructField("cons", StringType(), True)
])

# 3. Read the heavy CSV one last time
print("Reading CSV...")
df = spark.read.csv("../mergedFinal.csv", schema=schema, 
                    header=False, multiLine=True, quote='"', escape='"')

# 4. Save it as a Parquet file
print("Saving as Parquet... This might take a minute.")
df.write.mode("overwrite").parquet("../mergedFinal.parquet")

print("Done! You can now query the Parquet file.")
spark.stop()
