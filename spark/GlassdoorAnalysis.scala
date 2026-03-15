import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

/**
 * Glassdoor RDD Analysis in Scala
 * 
 * This script calculates weighted ratings for firms with > 1000 reviews.
 * It uses the same formula and column indices as your Python RDD script.
 */
object GlassdoorAnalysis {
  def main(args: Array[String]): Unit = {
    // If no argument is provided via props, we look at the main args
    val inputFile = if (args.length > 0) args(0) else {
      System.err.println("No input file provided.")
      return
    }

    // Configuration optimized for local execution
    val conf = new SparkConf()
      .setAppName("GlassdoorScalaRDD")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    // Check if a SparkContext already exists (common in spark-shell)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

    println(s"--- Starting Scala Spark Job (Cores: ${sc.defaultParallelism}) ---")
    val startTime = System.currentTimeMillis()

    // 1. Load Data
    val lines = sc.textFile(inputFile)

    // 2. Filter Header (Efficiently skip first line of first partition)
    val dataLines = lines.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) iter.drop(1) else iter
    }

    // 3. MAP PHASE
    val mappedRdd = dataLines.mapPartitions { iter =>
      iter.flatMap { line =>
        val cols = line.split(",")
        if (cols.length >= 11) {
          val firm = cols(0)
          
          val maybeScores = Try {
            val ratings = (6 to 10).map(i => cols(i).trim)
            if (ratings.contains("NULL") || ratings.contains("")) throw new Exception("Invalid data")

            val wlb = ratings(0).toDouble
            val cv  = ratings(1).toDouble
            val co  = ratings(2).toDouble
            val cb  = ratings(3).toDouble
            val sm  = ratings(4).toDouble

            val weightedScore = (
              (wlb * 1.1197) + 
              (cv * 1.4247) + 
              (co + (co * -0.0129)) + 
              (cb * 1.2584) + 
              (sm + (sm * -0.3348))
            ) / 5.0
            
            (firm, (1, weightedScore))
          }
          maybeScores.toOption
        } else {
          None
        }
      }
    }

    // 4. REDUCE PHASE (Fixed type inference for Spark Shell)
    val reducedRdd = mappedRdd.reduceByKey((a: (Int, Double), b: (Int, Double)) => (a._1 + b._1, a._2 + b._2))

    // 5. ACTION & POST-PROCESSING
    val results = reducedRdd
      .filter { case (_, (count, _)) => count >= 1000 }
      .mapValues { case (count, totalScore) => (count, totalScore / count) }
      .collect()
      .sortBy(_._2._2)(Ordering[Double].reverse)
      .take(10)

    val endTime = System.currentTimeMillis()

    // 6. Display Results
    println(f"\n--- TOP 10 WEIGHTED COMPANIES (SCALA RDD) ---")
    println(s"+${"-"*30}+${"-"*14}+${"-"*20}+")
    println(f"| ${"firm"}%-28s | ${"review_count"}%-12s | ${"weighted_rating"}%-18s |")
    println(s"+${"-"*30}+${"-"*14}+${"-"*20}+")
    
    results.foreach { case (firm, (count, avgRating)) =>
      val displayFirm = if (firm.length > 28) firm.substring(0, 25) + "..." else firm
      println(f"| $displayFirm%-28s | $count%-12d | $avgRating%-18.15f |")
    }
    println(s"+${"-"*30}+${"-"*14}+${"-"*20}+")
    
    println(f"\nScala Execution Time: ${(endTime - startTime) / 1000.0}%.2f seconds")
  }
}

// Execution block for spark-shell
val inputPath = sys.props.getOrElse("input", "mergedFinal.csv")
GlassdoorAnalysis.main(Array(inputPath))
