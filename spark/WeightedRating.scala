import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

/**
 * COMP30770 - Weighted Rating Calculation (Native Scala RDD Version)
 * 
 * Goal: Replicate the weighted rating logic of the 'Fat Data Dudes' project
 * to demonstrate the performance gain of a native JVM implementation.
 */
object WeightedRating {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: WeightedRating <input_file>")
      System.exit(1)
    }

    val inputFile = args(0)

    // Configuration for High Performance
    val conf = new SparkConf()
      .setAppName("GlassdoorScalaRDD")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println(s"--- Starting Scala Spark Job (Cores: ${sc.defaultParallelism}) ---")
    val startTime = System.currentTimeMillis()

    // 1. Load Data
    val lines = sc.textFile(inputFile)

    // 2. Filter Header (Efficiently)
    val dataLines = lines.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) iter.drop(1) else iter
    }

    // 3. MAP PHASE (The MapReduce Logic)
    val mappedRdd = dataLines.mapPartitions { iter =>
      iter.flatMap { line =>
        // Using a basic split for performance, similar to the standard Python approach
        val cols = line.split(",")
        if (cols.length >= 12) {
          val firm = cols(0)
          
          // Attempt to extract the 5 rating columns: 6, 7, 9, 10, 11
          val maybeScores = Try {
            val wlb = cols(6).toDouble
            val cv  = cols(7).toDouble
            val co  = cols(9).toDouble
            val cb  = cols(10).toDouble
            val sm  = cols(11).toDouble

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

    // 4. REDUCE PHASE (Map-Side Combining is Automatic in Scala)
    val reducedRdd = mappedRdd.reduceByKey { case ((c1, s1), (c2, s2)) =>
      (c1 + c2, s1 + s2)
    }

    // 5. Post-Processing: Filtering & Average
    val top10 = reducedRdd
      .filter { case (_, (count, _)) => count >= 1000 }
      .mapValues { case (count, totalScore) => (count, totalScore / count) }
      .sortBy(_._2._2, ascending = false)
      .take(10)

    val endTime = System.currentTimeMillis()

    // 6. Display Results
    println(s"\n--- TOP 10 WEIGHTED COMPANIES (SCALA RDD) ---")
    println(s"+${"-"*35}+${"-"*14}+${"-"*20}+")
    println(f"| ${"Firm Name"}%-33s | ${"Valid Reviews"}%-12s | ${"Weighted Rating"}%-18s |")
    println(s"+${"-"*35}+${"-"*14}+${"-"*20}+")
    
    top10.foreach { case (firm, (count, avgRating)) =>
      val displayFirm = if (firm.length > 33) firm.substring(0, 30) + "..." else firm
      println(f"| $displayFirm%-33s | $count%-12d | $avgRating%-18.15f |")
    }
    println(s"+${"-"*35}+${"-"*14}+${"-"*20}+")
    
    println(f"\nScala Execution Time: ${(endTime - startTime) / 1000.0}%.2f seconds")

    sc.stop()
  }
}
WeightedRating.main(Array("mergedFinal.csv"))
:quit
