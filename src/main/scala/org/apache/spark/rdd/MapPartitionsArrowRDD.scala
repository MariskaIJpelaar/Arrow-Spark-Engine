package org.apache.spark.rdd

import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect._

/**
 *
 * This class contains the equivalent of MapPartitionsRDD[T] that can work with ArrowRDD's, in order
 * to preserve ValueVector as data type for the RDD's until the very last stage of transformations
 * (such as data retrieval, using the ArrowPartition iterator)
 */
private[spark] class MapPartitionsArrowRDD[U: ClassTag, T: ClassTag]
                        (var par : ArrowRDD[T], f : (TaskContext, Int, Iterator[T]) => Iterator[U])
//                        extends ArrowRDD[U](par) with Logging{
                          extends RDD[U](par) with Logging{

  override def getPartitions : Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    logInfo("Called COMPUTE method on MapPartitionsArrowRDD %s, %s %s %s"
      .format(id, split.index, {classTag[T].runtimeClass}, {classTag[U].runtimeClass}))
    logInfo("Function call: %s".format(f.toString()))
    f(context, split.index, firstParent[T].iterator(split, context))
  }
}
