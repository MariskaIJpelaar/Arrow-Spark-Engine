package org.apache.spark.rdd

import org.apache.arrow.vector._
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.spark.internal.Logging
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect._


/* This class works the same as ParallelCollectionRDD, used in the sc.parallelize(Seq[T]) method
* and it uses Arrow-backed value vectors instead of the usual Scala Seq[T] collection */
private [spark] class ArrowRDD[T: ClassTag](@transient sc: SparkContext,
                            @transient private val data: ValueVector,
                            numSlices: Int,
                            locationPrefs: Map[Int, Seq[String]]) extends RDD[T](sc, Nil) with Logging{

//  def this(@transient oneParent : ArrowRDD[_]) =
//    this(oneParent.context)

  /* Needed to specify the actual concrete class that implements @data, to be used accordingly */
  val _type = data.getMinorType
  logInfo("ARROW RDD DATA TYPE: %s".format(_type.toString))
  logInfo("ARROW RDD CLASS TYPE: %s".format({classTag[T].runtimeClass}))

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    /* Specific hack: ArrowPartition is divided into specific working cases based on vector type
    (ArrowLongPartition and ArrowBinaryPartition), need to convert back to T to match signature */
    logInfo("Called compute on ARROW RDD")
    _type match {
      case MinorType.BIGINT =>
        new InterruptibleIterator(context, split.asInstanceOf[ArrowLongPartition].iterator.asInstanceOf[Iterator[T]])
      case MinorType.VARBINARY =>
        new InterruptibleIterator(context, split.asInstanceOf[ArrowBinaryPartition].iterator.asInstanceOf[Iterator[T]])
      case _ => throw new Exception("Unsupported Arrow Vector")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val slices = ArrowRDD.slice[T](data, numSlices).toArray
    logInfo("Called getPartitions on ARROW RDD")
    slices.indices.map(i => {
      _type match {
        case MinorType.BIGINT => new ArrowLongPartition(id, i, slices(i).asInstanceOf[BigIntVector])
        case MinorType.VARBINARY => new ArrowBinaryPartition(id, i, slices(i).asInstanceOf[VarBinaryVector])
        case _ => throw new Exception("Unsupported Arrow Vector")
      }
    }).toArray
  }

  override def getPreferredLocations(s: Partition) : Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }

  override def map[U: ClassTag](f: T => U): RDD[U] =  {
    val cleanF = sc.clean(f)
    new MapPartitionsArrowRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }
}

private object ArrowRDD {

  def slice[T: ClassTag](vec: ValueVector,
                         numSlices: Int): Seq[ValueVector] = {

    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt

        (start, end)
      }
    }

    /**
     * Slice original ValueVector with zero-copy, using the Slicing operations described
     * in the Java Arrow API documentation page.
     * Then convert the resulting vectors in a sequence
     */
    positions(vec.getValueCount, numSlices).map {
      case (start, end) => {
        val allocator = vec.getAllocator
        val tp = vec.getTransferPair(allocator)

        tp.splitAndTransfer(start, end-start)
        tp.getTo
      }
    }.toSeq
  }
}

