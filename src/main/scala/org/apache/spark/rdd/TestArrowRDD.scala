package org.apache.spark.rdd

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

private[spark] class TestArrowRDD[T: ClassTag](sc: SparkContext,
                                              @transient private val data: Array[ValueVector],
                                               numSlices: Int) extends RDD[T](sc, Nil) with Logging {

  private val _len = data.length
  require(_len <= 2, "Required maximum two ValueVector as parameter")
  private val _types = data.indices.iterator.map(i => data(i).getMinorType).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    logInfo("Length of array: %d".format(_len))
    _len match {
      case 1 => new InterruptibleIterator(context, split.asInstanceOf[ArrowPartition].iterator)
      case 2 => new InterruptibleIterator(context, split.asInstanceOf[ArrowCompositePartition].iterator.asInstanceOf[Iterator[T]])
      case _ => throw new SparkException("Wrong length")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    _len match {
      case 1 =>
        val slices = TestArrowRDD.slice(data(0), numSlices)
        slices.indices.map(i => new ArrowPartition(id, i, slices(i))).toArray
      case 2 =>
        val tupleSliced = TestArrowRDD.sliceAndTuplify(data, numSlices)
        tupleSliced.indices.map(i => new ArrowCompositePartition(id, i, tupleSliced(i))).toArray
      case _ => throw new SparkException("Required maximum two ValueVector as parameter")
    }
  }
}

private object TestArrowRDD {

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

  def sliceAndTuplify[T: ClassTag](vectors: Array[ValueVector],
                         numSlices: Int) : Seq[(ValueVector, ValueVector)] = {
    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    val _arrayLen = vectors.length
    require(vectors(0).getValueCount == vectors(1).getValueCount, "Vectors need to be the same size")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt

        (start, end)
      }
    }

    //Both value vectors have the same length, so only one is used
    positions(vectors(0).getValueCount, numSlices).map {
      case (start, end) => {
        val alloc1 = vectors(0).getAllocator
        val alloc2 = vectors(1).getAllocator
        val tp1 = vectors(0).getTransferPair(alloc1)
        val tp2 = vectors(1).getTransferPair(alloc2)

        tp1.splitAndTransfer(start, end-start)
        tp2.splitAndTransfer(start, end-start)

        (tp1.getTo, tp2.getTo)
      }
    }.toSeq
  }
}