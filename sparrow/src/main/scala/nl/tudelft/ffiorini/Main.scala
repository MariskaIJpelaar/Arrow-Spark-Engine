package nl.tudelft.ffiorini

import nl.tudelft.ffiorini.experiments.EvaluationSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ArrowSparkContext, SparkConf}
import picocli.CommandLine

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable
import scala.reflect.io.Directory
import scala.sys.exit

object Main {
  def main(args: Array[String]): Unit = {
    new CommandLine(new Main()).execute(args:_*)
  }
}

class Main extends Callable[Unit] {
  @picocli.CommandLine.Option(names = Array("-d", "--data-dir"))
  private var data_dir: String = ""
  @picocli.CommandLine.Option(names = Array("-f", "--data-file"))
  private var data_file: String = ""
  @picocli.CommandLine.Option(names = Array("-l", "--local"))
  private var local: Boolean = false
  @picocli.CommandLine.Option(names = Array("--spark-local-dir"))
  private var sparkLocalDir: String = "/tmp/"
  @picocli.CommandLine.Option(names = Array("-c", "--cache", "--cache-warmer"))
  private var cache_warmer: Int = 5
  @picocli.CommandLine.Option(names = Array("-r", "--runs", "--nr-runs"))
  private var nr_runs: Int = 30
  @picocli.CommandLine.Option(names = Array("--log-dir"))
  private var log_dir: Path = Paths.get("", "output")
  @picocli.CommandLine.Option(names = Array("--log-file"))
  private var log_file: String = "exp" + ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ".log"

  override def call(): Unit = {
    // User input checks
    if (data_dir == "" && data_file == "") {
      println("[ERROR] please provide a directory or file")
      exit(1)
    }
    if (data_dir != "" && data_file != "") {
      println("[ERROR] please provide either a directory or file")
      exit(1)
    }
    if (data_file != "" && !scala.reflect.io.File(data_file).exists) {
      println(s"[ERROR] $data_file does not exist")
      exit(1)
    }
    if (data_dir != "" && !Directory(data_dir).exists) {
      println(s"[ERROR] $data_dir does not exist")
      exit(1)
    }

    try {
      val start: Long = System.nanoTime()
      val conf = new SparkConf()
        .setAppName("Example Program")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "3048576")
        .set("spark.local.dir", sparkLocalDir)
      if (local)
        conf.setMaster("local")
      val sc = new ArrowSparkContext(conf)
      sc.setLogLevel("ERROR")
      val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

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
      if (data_file != "")
        fw.write(s"# File used: $data_file\n")
      else if (data_dir != "")
        fw.write(s"# Directory used: $data_dir\n")

      /**
       * Run the actual experiments
       */
      0 until nr_runs foreach { _ =>
        if (data_dir != "")
          EvaluationSuite.minimumValue(spark, sc, fw, Directory(data_dir))
        else if (data_file != "")
          EvaluationSuite.minimumValue(spark, sc, fw, data_file)
      }

      fw.close()
      println(s"Experiment took %04.3f seconds".format((System.nanoTime()-start)/1e9d))
    } catch {
      case e: Throwable => e.printStackTrace(); exit(1)
    }
  }
}