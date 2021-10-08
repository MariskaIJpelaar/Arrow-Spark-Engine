package nl.tudelft.ffiorini

import com.github.animeshtrivedi.arrowexample.ParquetToArrow
import org.apache.arrow.memory
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.parquet.ParquetToArrowConverter
import org.apache.arrow.vector.{StringVector, ValueVector}
import org.apache.log4j.BasicConfigurator
import org.apache.spark.rdd.ArrowPartition
import org.apache.spark.{ArrowSparkContext, SparkConf}

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:/hadoop")
    BasicConfigurator.configure()
    val conf = new SparkConf()
      .setAppName("Example Program")
      .setMaster("local")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "3048576")
    val sc = new ArrowSparkContext(conf)

//    val pta1 = new ParquetToArrowConverter
//    pta1.process("data/5-million-int-triples-snappy.parquet")
//
//
//    val pta3 = new ParquetToArrowConverter
//    pta3.process("data/taxi-uncompressed-10000.parquet")

    val handler = new ParquetToArrow
    handler.setParquetInputFile("data/taxi-uncompressed-10000.parquet")
    handler.process()

//    val lv1 = pta2.getBigIntVector.get()
//    val lv2 = pta3.getBigIntVector.get()
//
//    val la1 = Array[ValueVector](lv1)
//    val la2 = Array[ValueVector](lv2)
//
//    val t0 = System.nanoTime()
//    val rdd1 = sc.makeArrowRDD[Long](la2, 10)
//    val t1 = System.nanoTime()
//
//    val rt1 = (t1 - t0) / 1e9d
//    println("RDD CREATION: "+rt1)
//
//    val t00 = System.nanoTime()
//    val r1 = rdd1.map(x => (x, x.toString))
//    val t11 = System.nanoTime()
//
//    val rt11 = (t11 - t00) / 1e9d
//    println("TRANSFORMATION: "+rt11)
//
//    println(r1.first())

    val binVector = handler.getBinaryVector.get()
    val binArr = Array[ValueVector](binVector)

    val t0 = System.nanoTime()
    val binRDD = sc.makeArrowRDD[Array[Byte]](binArr, 10)
    val t1 = System.nanoTime()
    val result = binRDD.map(x => new String(x, StandardCharsets.UTF_8))
    val t2 = System.nanoTime()

    println("FIRST: "+result.first())
    println("TYPE: "+result.toString)
    println("NO. PARTITIONS: "+result.getNumPartitions)
    println("PARTITION TYPES: ")
    result.partitions.foreach(x => println(x.asInstanceOf[ArrowPartition].getVector.getMinorType))
//    println("COLLECT: ")
//    result.collect().foreach(println)
////    println("PARTITION DATA: "
////      +result.partitions.foreach(x =>
////      println(x.asInstanceOf[ArrowPartition].getVector.getMinorType+" "+x.asInstanceOf[ArrowPartition].getVector.getValueCount)))
//
    val ttime = (t2 - t1) / 1e9d
    val rddt = (t1 - t0) / 1e9d
    println("TRANSFORMATION TIME: "+ttime)
    println("RDD CREATION TIME: "+rddt)

//    val result2 = binRDD.map(x => x.length)
//    result2.first()
//    val result1 = binRDD.map(x => (x, 1L))
//    result1.first()

    val t3 = System.nanoTime()
    val result1 = result.map(x => x.length)
    val t4 = System.nanoTime()

    val ttime2 = (t4 - t3) / 1e9d
    println("TRANSFORMATION TIME (2) "+ttime2)
    println("FIRST: "+result1.first())
    println("TYPE: "+result1.toString)
    println("NO. PARTITIONS: "+result1.getNumPartitions)
    println("PARTITION TYPES: ")
    result1.partitions.foreach(x => println(x.asInstanceOf[ArrowPartition].getVector.getMinorType))

//    val strv = new StringVector("vector", new RootAllocator(Long.MaxValue))
//    strv.allocateNew(10)
//    for (i <- 0 until 10) strv.set(i, "hello")
//    strv.setValueCount(10)
//
//    val strvArr = Array[ValueVector](strv)
//    val strdd = sc.makeArrowRDD[String](strvArr)
//
//    val result = strdd.map(x => x.length)
//    val result2 = result.map(x => x.toString)
//
//    println("CLASS (first element of result): "+result.first().getClass)
//    println("CLASS (first element of result2): "+result2.first().getClass)

//    val vsr1 = pta.getVectorSchemaRoot
//    val vsr2 = pco.getVectorSchemaRoot
//
//    println("SCHEMA1: "+vsr1.getSchema.toString)
//    println("SCHEMA2: "+vsr2.getSchema.toString)
//
//    val intVector = pta.getIntVector.get()
//    println("NO. ENTRIES: "+intVector.getValueCount)
//    val intArr = Array[ValueVector](intVector)
//
//    val intRDD = sc.makeArrowRDD[Int](intArr, 10)
//    val result1 = intRDD.map(x => (x,1L))
//    println("RESULT1 COUNT: "+result1.count())
//    println("RESULT1 FIRST: "+result1.first())
//
//    val result3 = intRDD.map(x => (x, x.toString))
//    println("RESULT3 COUNT: "+result3.count())
//    println("RESULT3 FIRST: "+result3.first())
//
//    val binVector = handler.getBinaryVector.get()
//    println("NO. ENTRIES (2): "+binVector.getValueCount)
//    val binArr = Array[ValueVector](binVector)
//
//    val binRDD = sc.makeArrowRDD[Array[Byte]](binArr, 10)
//    val result2 = binRDD.map(x => new String(x, StandardCharsets.UTF_8)).map(x => (x,1))
//    println("RESULT2 COUNT: "+result2.count())
//    println("RESULT2 FIRST: "+result2.first())

//    val data = Range(0, 100, 1).toArray
//    val rdd = sc.parallelize(data, 10)
//
//    val res = rdd.map(x => x.toString)
//
//    println("RDD INFO: "+res.toString+" "+res.count()+" "+res.getNumPartitions)
  }
}
