package org.apache.spark.rdd

import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *
 * This class contains the equivalent of MapPartitionsRDD[T] that can work with ArrowRDD's, in order
 * to preserve ValueVector as data type for the RDD's until the very last stage of transformations
 * (such as data retrieval, using the ArrowPartition iterator)
 */
private[spark] class MapPartitionsArrowRDD[U: ClassTag, T: ClassTag]
                        (var par : ArrowRDD[T], f : (TaskContext, Int, Iterator[T]) => Iterator[U])
                        (implicit tag : TypeTag[T], tag2 : TypeTag[U])
                        extends ArrowRDD[U](par.context, par.data, par.numSlices, par.locationPrefs) with Logging{

  /* Update 17.09: used for .filter() transformations */
  private var _preservePartitioning = false
  override val partitioner = if (_preservePartitioning) par.partitioner else None

  def setPreservePartitioning() : Unit = {
    _preservePartitioning = true
  }

  override def getPartitions : Array[Partition] = par.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    f(context, split.index, par.iterator(split, context))
  }
}
