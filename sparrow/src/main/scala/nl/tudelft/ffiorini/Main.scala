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
import scala.sys.exit

object Main {
  def main(args: Array[String]): Unit = {
    new CommandLine(new Main()).execute(args:_*)

    /* Run experiments here */
//      EvaluationSuite.wordCount(sc)
    
//      EvaluationSuite.scalaSort(sc)
//
//      EvaluationSuite.transformations(sc)
//
//      EvaluationSuite.minimumValue(sc)
  }
}

class Main extends Callable[Unit] {
  @picocli.CommandLine.Option(names = Array("-d", "--data-dir"))
  private var data_dir: Option[String] = None
  @picocli.CommandLine.Option(names = Array("-f", "--data-file"))
  private var data_file: Option[String] = None
  @picocli.CommandLine.Option(names = Array("-l", "--local"))
  private var local: Boolean = false
  override def call(): Unit = {
    if (data_dir.isEmpty || data_file.isEmpty ) {
      println("[ERROR] provide either a directory or a file")
      exit(1)
    }

    val start: Long = System.nanoTime()
    //      System.setProperty("hadoop.home.dir","C:/hadoop")
    val conf = new SparkConf()
      .setAppName("Example Program")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "3048576")
    if (local)
      conf.setMaster("local")
    val sc = new ArrowSparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    /**
     * NOTE: below we hardcode our configurations for a quick setup
     * If we expand this suite for more serius experimentation, we MUST setup a more modular method
     */
    val cache_warmer: Int = 5
    val nr_runs: Int = 30
    val log_dir: Path = Paths.get("", "output")
    val log_file: String = "exp" + ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ".log"
    // id, file, range
    val inputs: Array[(String, String)] = Array(
      ("100k", s"$data_dir/numbers_100k.parquet"),
      ("1m", s"$data_dir/numbers_1m.parquet"),
      ("10m", s"$data_dir/numbers_10m.parquet")
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
      if (data_dir.isDefined) {
        inputs.foreach { case(id, file) =>
          fw.write(s"# Results for file $file, with id $id\n")
          fw.write(s"ID: $id\n")
          EvaluationSuite.minimumValue(spark, sc, fw, file)
        }
      }
      if (data_file.isDefined) {
        fw.write(s"# Results for file $data_file")
        val name = new File(data_file.get).getName
        fw.write(s"ID: $name")
        EvaluationSuite.minimumValue(spark, sc, fw, data_file.get)
      }
    }

    fw.close()
    println(s"Experiment took %04.3f seconds".format((System.nanoTime()-start)/1e9d))
  }
}