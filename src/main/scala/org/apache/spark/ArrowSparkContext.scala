package org.apache.spark

import org.apache.arrow.vector.ValueVector
import org.apache.spark.rdd.{ArrowRDD, RDD}

import scala.reflect.ClassTag

class ArrowSparkContext(config: SparkConf) extends SparkContext(config) {
  /**
   * Container class for extended def parallelize(...) method using ValueVector
   * instead of Seq[T].
   *
   * Need to try that out first, then place it in the actual SparkContext once working
   */
  def parallelizeWithArrow[T: ClassTag](vector: ValueVector,
                                numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ArrowRDD[T](this, vector, numSlices, Map[Int, Seq[String]]())
  }
}
