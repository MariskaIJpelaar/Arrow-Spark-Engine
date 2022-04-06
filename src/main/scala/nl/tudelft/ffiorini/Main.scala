package nl.tudelft.ffiorini

import nl.tudelft.ffiorini.experiments.EvaluationSuite
import org.apache.spark.{ArrowSparkContext, SparkConf}

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object Main {
  def main(args: Array[String]): Unit = {
//      System.setProperty("hadoop.home.dir","C:/hadoop")
      val conf = new SparkConf()
        .setAppName("Example Program")
//        .setMaster("local")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "3048576")
      val sc = new ArrowSparkContext(conf)
      sc.setLogLevel("ERROR")

    /**
     * NOTE: below we hardcode our configurations for a quick setup
     * If we expand this suite for more serius experimentation, we MUST setup a more modular method
     */
    val cache_warmer: Int = 5
    val nr_runs: Int = 1
    val log_dir: Path = Paths.get("", "output")
    val log_file: String = "exp" + ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ".log"
    // id, file, range
    val inputs: Array[(String, String, Int)] = Array(
      ("100k", "data/data/numbers_100k.parquet", 100 * 1000),
      ("1m", "data/data/numbers_1m.parquet", 1000 * 1000),
      ("10m", "data/data/numbers_10m.parquet", 10 * 1000 * 1000)
    )

    /**
     * Warm up cache with a simple (vanilla) program
     */
    0 until cache_warmer foreach { _ =>
      sc.parallelize(Range(0, 100 * 1000, 1), 10).min()
    }

    /**
     * Setup Log file
     */
    new File(log_dir.toAbsolutePath.toString).mkdir() // create directory if it does not exist yet
    val write_file = log_dir.resolve(log_file)
    Files.write(write_file, "".getBytes(StandardCharsets.UTF_8)) // clear file
    val fw = new FileWriter(write_file.toFile, true) // append to log file
    fw.write(s"# Experiment repeated $nr_runs times, with running times in seconds\n")

    /**
     * Run the actual experiments
     */
    0 until nr_runs foreach { _ =>
      inputs.foreach { case(id, file, range) =>
        fw.write(s"# Results for file $file and range $range, with id $id\n")
        fw.write(s"ID: $id\n")
        EvaluationSuite.minimumValue(sc, fw, file, range)
      }
    }

    fw.close()

    /* Run experiments here */
//      EvaluationSuite.wordCount(sc)
    
//      EvaluationSuite.scalaSort(sc)
//
//      EvaluationSuite.transformations(sc)
//
//      EvaluationSuite.minimumValue(sc)
  }
}
