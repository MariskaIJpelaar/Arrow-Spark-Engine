package nl.tudelft.ffiorini

import com.github.animeshtrivedi.arrowexample.ParquetToArrow
import com.intel.hibench.sparkbench.common.IOCommon
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

//    val sc = new SparkContext(conf)
    val sc = new ArrowSparkContext(conf)

    val parquetHandler: ParquetToArrow = new ParquetToArrow()
    parquetHandler.setParquetInputFile("data/taxi-uncompressed-10000.parquet")
    parquetHandler.process()

    val setupTime = (System.nanoTime() - t0) / 1e9d

    val t1 = System.nanoTime()
//    val binVector = parquetHandler.getBinaryVector.get()
    val longVector = parquetHandler.getInteger64Vector.get()
//    val binrdd2 = sc.parallelizeWithArrow[Array[Byte]](binVector, 10 )
    val longRdd = sc.parallelizeWithArrow[Long](longVector, 10)
//    println("RDD COUNT: " +binrdd2.count())
//    val data = sc.textFile("/data/example.txt")

    val rddTime = (System.nanoTime() - t1) / 1e9d

    val t2 = System.nanoTime()
//    val result = binrdd2.map(x => new String(x, StandardCharsets.UTF_8)).map(x => (x,1))
//    val counts = data.flatMap(line => line.split(" ")).map(word => (word, 1))
    val result = longRdd.map(x => (x, "right"))
    println("RDD COUNT: " +result.count())
    val transformTime = (System.nanoTime() - t2) / 1e9d

    println("PREPARATION TIME: " +setupTime+ " s")
    println("RDD CREATION TIME: " +rddTime+ " s")
    println("TRANSFORMATION TIME: " +transformTime+ " s")
  }
}
