package org.apache.spark.rdd

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 *
 * This class contains the equivalent of MapPartitionsRDD[T] that can work with ArrowRDD's, in order
 * to preserve ValueVector as data type for the RDD's until the very last stage of transformations
 * (such as data retrieval, using the ArrowPartition iterator)
 */
private[spark] class MapPartitionsArrowRDD[U: ClassTag, T: ClassTag]
                        (var par : ArrowRDD[T], f : (TaskContext, Int, Iterator[T]) => Iterator[U])
                        extends RDD[U](par){

  override def getPartitions : Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {

    f(context, split.index, firstParent[T].iterator(split, context))
  }
}
