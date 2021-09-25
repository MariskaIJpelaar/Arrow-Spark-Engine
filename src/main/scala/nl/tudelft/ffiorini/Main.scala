package nl.tudelft.ffiorini

import com.github.animeshtrivedi.arrowexample.ParquetToArrow
import org.apache.arrow.parquet.ParquetToArrowConverter
import org.apache.arrow.vector.ValueVector
import org.apache.log4j.BasicConfigurator
import org.apache.spark.{ArrowSparkContext, SparkConf}

import java.nio.charset.StandardCharsets

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
//    val pta2 = new ParquetToArrowConverter
//    pta2.process("data/big-example.parquet")
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

    val ttime = (t2 - t1) / 1e9d
    val rddt = (t1 - t0) / 1e9d
    println("TIMING: "+ttime)
    println("RDD: "+rddt)

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

    val data = Range(0, 100, 1).toArray
    val rdd = sc.parallelize(data, 10)

    val res = rdd.map(x => x.toString)
//
//    println("RDD INFO: "+res.toString+" "+res.count()+" "+res.getNumPartitions)
  }
}
