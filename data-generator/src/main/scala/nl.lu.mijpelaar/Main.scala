package nl.lu.mijpelaar

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import picocli.CommandLine

import java.io.File
import java.util.concurrent.Callable
import scala.reflect.io.Directory

object Main {
  def main(args: Array[String]): Unit = {
    new CommandLine(new Main()).execute(args: _*)
  }
}

class Main extends Callable[Unit] {
  @picocli.CommandLine.Option(names = Array("-a", "--amount"))
  private var amount: Int = 100
  @picocli.CommandLine.Option(names = Array("-l", "--local"))
  private var local: Boolean = false
  @picocli.CommandLine.Option(names = Array("-p", "--path"))
  private var path: String = "data/generated_" + amount
  @picocli.CommandLine.Option(names = Array("-n", "--num-files"))
  private var numFiles: Int = 1
  @picocli.CommandLine.Option(names = Array("--spark-local-dir"))
  private var sparkLocalDir: String = "/tmp/"

  override def call(): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      println(s"[ERROR] Directory $path already exists")
      return
    }

    val sparkBuilder = SparkSession.builder.appName("generator")
      .config("spark.local.dir", sparkLocalDir)
    if (local)
      sparkBuilder.config("spark.master", "local[*]")
    val spark = sparkBuilder.getOrCreate()

    /**
     * according to: https://spark.apache.org/docs/2.1.0/programming-guide.html#parallelized-collections
     * "One important parameter for parallel collections is the number of partitions
     * to cut the dataset into. Spark will run one task for each partition of the
     * cluster. Typically you want 2-4 partitions for each CPU in your cluster.
     * Normally, Spark tries to set the number of partitions automatically based on
     * your cluster. However, you can also set it manually by passing it as a second
     * parameter to parallelize (e.g. sc.parallelize(data, 10))."
     *
     * Thus, we keep the second argument as default as we trust Spark :)
     */
    val intRDD = spark.sparkContext.parallelize(Range(0, amount, 1).map(x => Row(x)))
    val schema = new StructType().add(StructField("num", IntegerType, nullable = false))
    spark.createDataFrame(intRDD, schema).repartition(numFiles).write.parquet(path)

    /**
     * We have no influence over the parquet-filename, so we rename it ourselves...
     */
    if (dir.exists && dir.isDirectory) {
      val files = dir.listFiles.filter(_.isFile).filter(file => FilenameUtils.getExtension(file.getName) == "parquet").toList
      files.zipWithIndex.foreach { case (file, idx) =>
        file.renameTo(new File(path + s"_$idx.parquet"))
      }
      new Directory(dir).deleteRecursively()
    } else {
      println("[ERROR] Something went wrong with generating the file(s)")
    }
  }
}