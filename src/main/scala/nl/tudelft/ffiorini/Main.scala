package nl.tudelft.ffiorini

import com.github.animeshtrivedi.arrowexample.ParquetToArrow
import org.apache.arrow.parquet.ParquetToArrowConverter
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.spark.{ArrowSparkContext, SparkConf}

import java.nio.charset.StandardCharsets
import scala.reflect.{ClassTag, classTag}

object Main {
  def main(args: Array[String]): Unit = {
      System.setProperty("hadoop.home.dir","C:/hadoop")
      val conf = new SparkConf()
        .setAppName("Example Program")
        .setMaster("local")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "3048576")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir","logs")
      val sc = new ArrowSparkContext(conf)

      val t0 = System.nanoTime()
      val textRDD = sc.textFile("data/example_10k.txt", 10)
      val t1 = System.nanoTime()
      val textResult1 = textRDD.flatMap(x => x.split(" ")).map(x => (x,1))
      textResult1.first()
      val t2 = System.nanoTime()
      val textResult2 = textResult1.reduceByKey(_ + _)
      textResult2.first()
      val t3 = System.nanoTime()

      val t4 = System.nanoTime()
      val handler = new ParquetToArrow
      handler.setParquetInputFile("data/taxi-uncompressed-10000.parquet")
      handler.process()
      val binVector = handler.getBinaryVector.get()
      val binArray = Array[ValueVector](binVector)
      val t5 = System.nanoTime()
      val binRDD = sc.makeArrowRDD[Array[Byte]](binArray, 10)
      val t6 = System.nanoTime()
      val binResult1 = binRDD.map(x => new String(x, StandardCharsets.UTF_8)).map(x => (x,1))
      binResult1.first()
      val t7 = System.nanoTime()
      val binResult2 = binResult1.reduceByKey(_ + _)
      binResult2.first()
      val t8 = System.nanoTime()

      val oldRdd = (t1-t0)/1e9d
      val oldNarrow = (t2-t1)/1e9d
      val oldWide = (t3-t2)/1e9d
      val oldTotal = (t3-t0)/1e9d

      val newPq = (t5-t4)/1e9d
      val newRdd = (t6-t5)/1e9d
      val newNarrow = (t7-t6)/1e9d
      val newWide = (t8-t7)/1e9d
      val newTotPq = (t8-t4)/1e9d
      val newTotNo = (t8-t5)/1e9d

      println("TIMING (SPARK): %04.3f + %04.3f +%04.3f = %04.3f"
                .format(oldRdd, oldNarrow, oldWide, oldTotal))
      println("TIMING (ARROW-SPARK): %04.3f + %04.3f + %04.3f + %04.3f = (%04.3f) %04.3f"
                .format(newPq, newRdd, newNarrow, newWide, newTotNo, newTotPq))

//    val start1 = sc.textFile("data/example-10m.txt",100).map(x => x.length)
//    val t0 = System.nanoTime()
//    val res1 = start1.map(x => x + 5)
////    val res1 = start1.map(x => x.toString)
//    res1.first()
//    val t1 = System.nanoTime()
//
//    val handler = new ParquetToArrowConverter
//    handler.process("data/numbers_10m.parquet")
//    val arr = Array[ValueVector](handler.getIntVector.get())
//    val start2 = sc.makeArrowRDD[Int](arr,100)
//    val t2 = System.nanoTime()
//    val res2 = start2.map(x => x + 5)
////    val res2 = start2.map(x => x.toString)
//    res2.first()
//    val t3 = System.nanoTime()
//
//    val oldT = (t1-t0)/1e9d
//    val newT = (t3-t2)/1e9d
//
//    println("%04.3f     %04.3f".format(oldT, newT))
  }
}
