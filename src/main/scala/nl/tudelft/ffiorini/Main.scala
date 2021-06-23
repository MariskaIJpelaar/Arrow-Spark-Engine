package nl.tudelft.ffiorini

import com.github.animeshtrivedi.arrowexample.ParquetToArrow
import org.apache.arrow.vector.BaseVariableWidthVector
import org.apache.log4j.BasicConfigurator
import org.apache.spark.rdd.ArrowPartition
import org.apache.spark.{ArrowSparkContext, SparkConf, SparkContext}

import java.nio.charset.StandardCharsets

object Main {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:/hadoop")
    BasicConfigurator.configure()
    val conf = new SparkConf()
      .setAppName("Example Program")
      .setMaster("local")

    val t0 = System.nanoTime()
    //val sc = new SparkContext(conf)
    val sc = new ArrowSparkContext(conf)

    val parquetHandler: ParquetToArrow = new ParquetToArrow()
    parquetHandler.setParquetInputFile("data/taxi-uncompressed-10000.parquet")
    parquetHandler.process()

    val setupTime = (System.nanoTime() - t0) / 1e9d

    val t1 = System.nanoTime()
    val binVector = parquetHandler.getBinaryVector.get()

    val binrdd2 = sc.parallelizeWithArrow[Array[Byte]](binVector, 5)
    val result = binrdd2.map(x => new String(x, StandardCharsets.UTF_8)).map(x => (x,1))

    val transformTime = (System.nanoTime() - t1) / 1e9d

    println("PREPARATION TIME: " +setupTime+ " s")
    println("TRANSFORMATION TIME: " +transformTime+ " s")
  }
}
