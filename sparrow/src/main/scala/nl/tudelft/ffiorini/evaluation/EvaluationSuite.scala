package nl.tudelft.ffiorini.experiments

import io.netty.util.internal.PlatformDependent
import org.apache.arrow.parquet.ParquetToArrowConverter
import org.apache.arrow.vector.ValueVector
import org.apache.spark.ArrowSparkContext
import org.apache.spark.sql.SparkSession

import java.io.FileWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.reflect.io.Directory

object EvaluationSuite {

  def wordCount(sc : ArrowSparkContext) : Unit = {
    println("WORDCOUNT EXAMPLE")
    val numPart = 10

    val start_vanilla: Long = System.nanoTime()
    val textRDD = sc.textFile("data/data/example-10m.txt", numPart)
    textRDD.first()
    val narrTr = textRDD.flatMap(line => line.split(" ")).map(word => (word, 1))
    narrTr.first()
    val wideTr = narrTr.reduceByKey(_ + _)
    wideTr.first()
    println("Vanilla: %04.3f".format((System.nanoTime()-start_vanilla)/1e9d))

    val start_sparrow: Long = System.nanoTime()
    val handler = new ParquetToArrowConverter
    handler.process("data/data/people10m.parquet")
    val binArr = Array[ValueVector](handler.getVariableWidthVector.get())
    val binRDD = sc.makeArrowRDD[Array[Byte]](binArr, numPart)
    binRDD.first()
    val binNarrTr = binRDD.map(b => new String(b, StandardCharsets.UTF_8)).map(word => (word, 1))
    binNarrTr.first()
    val binWideTr = binNarrTr.reduceByKey(_ + _)
    binWideTr.first()
    println("SpArrow: %04.3f".format((System.nanoTime()-start_sparrow)/1e9d))

    println("END OF WORDCOUNT EXAMPLE")
  }

  def scalaSort(sc: ArrowSparkContext) : Unit = {
    println("SCALASORT EXAMPLE")
    val numPart = 10

    val textRDD = sc.textFile("data/example-10m.txt", numPart)
    textRDD.first()
    val narrTr = textRDD.map((_, 1))
    narrTr.first()
    val wideTr = narrTr.sortByKey(true).map(_._1)
    wideTr.first()

    val handler = new ParquetToArrowConverter
    handler.process("data/people10m.parquet")
    val binArr = Array[ValueVector](handler.getVariableWidthVector.get())
    val binRDD = sc.makeArrowRDD[Array[Byte]](binArr, numPart)
    binRDD.first()
    val binNarrTr = binRDD.map(b => new String(b, StandardCharsets.UTF_8)).map((_, 1))
    binNarrTr.first()
    val binWideTr = binNarrTr.sortByKey(true).map(_._1)
    binWideTr.first()

    println("END OF SCALASORT EXAMPLE")
  }

  def minimumValue(spark: SparkSession, sc: ArrowSparkContext, fw: FileWriter, dir: Directory, numPart: Int) : Unit = {
//    val numPart = 10
    val tableName = "vanilla"

    // Vanilla Spark
    val start_vanilla_read: Long = System.nanoTime()
    spark.read.format("parquet").option("mergeSchema", "true").option("dbtable", tableName)
      .load(Paths.get(dir.toString()).resolve("*").toString)
      .createOrReplaceTempView(tableName)
    val intRDDVan = spark.table(tableName).rdd.map(x => x.getInt(0))
    fw.write("Vanilla Read: %04.3f\n".format((System.nanoTime()-start_vanilla_read)/1e9d))
    fw.flush()
    val start_vanilla_compute: Long = System.nanoTime()
    intRDDVan.min()
    fw.write("Vanilla Compute: %04.3f\n".format((System.nanoTime()-start_vanilla_compute)/1e9d))
    fw.flush()

    // SpArrow
    val start_sparrow_read: Long = System.nanoTime()
    val handler = new ParquetToArrowConverter
    handler.process(dir)
    println(s"ref A: ${handler.getVectorSchemaRoot.getVector(0).getDataBuffer.getReferenceManager.getRefCount}")
    val intArr = Array[ValueVector](handler.getIntVector.get())
    println(s"ref B: ${handler.getVectorSchemaRoot.getVector(0).getDataBuffer.getReferenceManager.getRefCount}")
    val intRDD = sc.makeArrowRDD[Int](intArr, numPart)
    println(s"ref C: ${handler.getVectorSchemaRoot.getVector(0).getDataBuffer.getReferenceManager.getRefCount}")
    fw.write("SpArrow Read: %04.3f\n".format((System.nanoTime()-start_sparrow_read)/1e9d))
    fw.flush()
    // TODO: ugly but might work...
    val refCounts = handler.getVectorSchemaRoot.getFieldVectors.toArray().map( vector =>
      vector.asInstanceOf[ValueVector].getDataBuffer.getReferenceManager.getRefCount)

    val start_sparrow_default_compute: Long = System.nanoTime()
    intRDD.min()

    // TODO: ugly but might work...
    refCounts.indices foreach { i =>
      val refManager = handler.getVectorSchemaRoot.getVector(i).getDataBuffer.getReferenceManager
      refManager.release(refManager.getRefCount - refCounts(i))
    }

    println(s"ref D: ${handler.getVectorSchemaRoot.getVector(0).getDataBuffer.getReferenceManager.getRefCount}")
    fw.write("SpArrow Compute Default: %04.3f\n".format((System.nanoTime()-start_sparrow_default_compute)/1e9d))
    fw.flush()
    val start_sparrow_offload_compute: Long = System.nanoTime()
    intRDD.vectorMin()
    println(s"ref E: ${handler.getVectorSchemaRoot.getVector(0).getDataBuffer.getReferenceManager.getRefCount}")
    fw.write("SpArrow Compute Offloading: %04.3f\n".format((System.nanoTime()-start_sparrow_offload_compute)/1e9d))
    fw.flush()

    // TODO: find automatic way
    println(s"before clear ${PlatformDependent.usedDirectMemory()}")
    handler.clear()
    println(s"after clear ${PlatformDependent.usedDirectMemory()}")
    intRDD.data.foreach { vector => vector.clear() }
  }

  def minimumValue(spark: SparkSession, sc: ArrowSparkContext, fw: FileWriter, file: String, numPart: Int) : Unit = {
//    val numPart = 10

    val start_vanilla_generate: Long = System.nanoTime()
    val intRDDVan = spark.read.parquet(file).rdd.map(x => x.getInt(0))
    fw.write("Vanilla Read: %04.3f\n".format((System.nanoTime()-start_vanilla_generate)/1e9d))
    fw.flush()
    val start_vanilla_compute: Long = System.nanoTime()
    intRDDVan.min()
    fw.write("Vanilla Compute: %04.3f\n".format((System.nanoTime()-start_vanilla_compute)/1e9d))
    fw.flush()

    val start_sparrow_generate: Long = System.nanoTime()
    val handler = new ParquetToArrowConverter
    handler.process(file)
    val intArr = Array[ValueVector](handler.getIntVector.get())
    val intRDD = sc.makeArrowRDD[Int](intArr, numPart)
    fw.write("SpArrow Read: %04.3f\n".format((System.nanoTime()-start_sparrow_generate)/1e9d))
    fw.flush()
    val start_sparrow_default_compute: Long = System.nanoTime()
    intRDD.min()
    fw.write("SpArrow Compute Default: %04.3f\n".format((System.nanoTime()-start_sparrow_default_compute)/1e9d))
    fw.flush()

    val start_sparrow_offload_compute: Long = System.nanoTime()
    intRDD.vectorMin()
    fw.write("SpArrow Compute Offloading: %04.3f\n".format((System.nanoTime()-start_sparrow_offload_compute)/1e9d))
    fw.flush()

    // TODO: find automatic way
    handler.clear()
    intRDD.data.foreach { vector => vector.clear() }
  }

  def transformations(sc: ArrowSparkContext) : Unit = {

  }
}