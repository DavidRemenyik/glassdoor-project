import sys
import os

from pyspark.sql import SparkSession
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

def parse_and_score(row):
    # Extract the firm name directly from the Row object
    firm = row.firm
    
    # Extract the rating columns
    wlb = row.work_life_balance
    cv = row.culture_values
    co = row.career_opp
    cb = row.comp_benefits
    sm = row.senior_mgmt
    
    # Filter out rows with NULLs (in Parquet, missing values become Python 'None')
    if None in [wlb, cv, co, cb, sm]:
        return ("FILTER_ME", (0, 0.0))
    
    try:
        wlb = float(wlb)
        cv = float(cv)
        co = float(co)
        cb = float(cb)
        sm = float(sm)
    except (ValueError, TypeError):
        return ("FILTER_ME", (0, 0.0))
    
    # Calculate the weighted score per your team's formula
    weighted_score = (
        (wlb * 1.1197) + 
        (cv * 1.4247) + 
        (co + (co * -0.0129)) + 
        (cb * 1.2584) + 
        (sm + (sm * -0.3348))
    ) / 5.0
    
    # Output Key-Value Pair: (Firm, (Count, Score))
    return (firm, (1, weighted_score))

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 rdd-parquet-spark.py <path_to_parquet_file_or_dir>")
        sys.exit(1)
        
    input_file = sys.argv[1]

    # Initialize SparkSession to read the Parquet file
    spark = SparkSession.builder \
        .appName("GlassdoorRDDMapReduceParquet") \
        .getOrCreate()
        
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Load Parquet and immediately convert it to an RDD to strictly use the Core API
    df = spark.read.parquet(input_file)
    data_rows = df.rdd

    # ==========================================
    # 1. MAP PHASE (Narrow Dependency)
    # ==========================================
    mapped_rdd = data_rows.map(parse_and_score).filter(lambda x: x[0] != "FILTER_ME")

    # ==========================================
    # 2. REDUCE PHASE (Wide Dependency)
    # ==========================================
    reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # ==========================================
    # 3. POST-PROCESSING & FILTERING
    # ==========================================
    # Keep only firms with >= 1000 reviews
    filtered_firms = reduced_rdd.filter(lambda x: x[1][0] >= 1000)

    # Calculate final average while keeping the count: (Count, Total Score / Count)
    final_with_counts = filtered_firms.mapValues(lambda v: (v[0], v[1] / v[0]))

    # ==========================================
    # 4. ACTION
    # ==========================================
    # Sort by the average rating in descending order
    sorted_with_counts = final_with_counts.sortBy(lambda x: x[1][1], ascending=False)
    top_10_final = sorted_with_counts.take(10)

    # Print results
    print("\n--- TOP 10 WEIGHTED COMPANIES (RDD CORE API w/ PARQUET) ---")
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+")
    print(f"| {'firm':<28} | {'review_count':<12} | {'weighted_rating':<18} |")
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+")
    
    for firm, (count, avg_rating) in top_10_final:
        display_firm = firm[:28] if firm and len(firm) > 28 else firm
        print(f"| {display_firm:<28} | {count:<12} | {avg_rating:<18.15f} |")
        
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+\n")

    spark.stop()

if __name__ == "__main__":
    main()
