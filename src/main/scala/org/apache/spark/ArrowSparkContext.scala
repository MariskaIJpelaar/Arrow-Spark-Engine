package org.apache.spark

import org.apache.arrow.vector.ValueVector
import org.apache.spark.rdd.{ArrowRDD, TestArrowRDD}

import scala.reflect.ClassTag

class ArrowSparkContext(config: SparkConf) extends SparkContext(config) {
  /**
   * Method to create ArrowRDD[T] starting off from an Arrow-backed ValueVector
   *
   * (27.07) tried to include it in the actual SparkContext but somehow there were
   * some troubles with "cannot resolve symbol ..." so I left it here. It works
   */
  def makeArrowRDD[T: ClassTag](@transient vector: ValueVector,
                                numSlices: Int = defaultParallelism): ArrowRDD[T] = withScope {
    assertNotStopped()
    new ArrowRDD[T](this, vector, numSlices, Map[Int, Seq[String]]())
  }

  def makeCompositeArrowRDD[T: ClassTag](@transient vectors: Array[ValueVector],
                                         numSlices: Int = defaultParallelism): TestArrowRDD[T] = withScope {
    assertNotStopped()
    new TestArrowRDD[T](this, vectors, numSlices)
  }
}
