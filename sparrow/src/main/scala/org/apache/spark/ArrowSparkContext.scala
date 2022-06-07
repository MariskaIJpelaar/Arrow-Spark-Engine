package org.apache.spark

import org.apache.arrow.vector.ValueVector
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.sparrow.ArrowRDD

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class ArrowSparkContext(config: SparkConf) extends SparkContext(config) {
  /**
   * Method to create ArrowRDD[T] starting off from an array of Arrow-backed ValueVector
   *
   * (27.07) tried to include it in the actual SparkContext but somehow there were
   * some troubles with "cannot resolve symbol ..." so I left it here. It works
   */
  def makeArrowRDD[T: ClassTag : TypeTag](@transient vectors: Array[ValueVector],
                                numSlices: Int = defaultParallelism): ArrowRDD[T] = withScope {
    assertNotStopped()
    new ArrowRDD[T](this, vectors, numSlices, Map[Int, Seq[String]]())
  }

  /**
   * Methods to 'cast' an RDD[T] back to an ArrowRDD[T]
   * Assumes the underlying RDD[T] is actually an ArrowRDD[T]
   * @param rdd the RDD to cast
   * @tparam T the type saved in the RDD
   * @return the rdd as an Instance of ArrowRDD[T]
   */
  def makeArrowRDD[T: ClassTag : TypeTag](rdd: RDD[T]): ArrowRDD[T] = withScope {
    assertNotStopped()
    rdd.asInstanceOf[ArrowRDD[T]]
  }

  /**
   * @return the nextRddId that will be used for RDD generation
   */
  def getNextRddId: Int = {
    // Using Java reflection to get the private member
    // inspiration: https://stackoverflow.com/questions/47038412/get-private-field-in-scala-trait-using-java-reflection
    val field = classOf[SparkContext].getDeclaredField("nextRddId")
    field.setAccessible(true)
    field.get(this).asInstanceOf[AtomicInteger].get()
  }
}
