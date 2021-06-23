package org.apache.spark.rdd

import org.apache.arrow.vector._
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.io._
import java.io.IOException
import scala.language.higherKinds
import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer

/*This class works the same as ParallelCollectionPartition, used in the ParallelCollectionRDD below
* and it uses Arrow-backed value vectors instead of the usual Scala Seq[T] collection */
class ArrowPartition[T: ClassTag](var rddId: Long,
                                  var slice: Int,
                                  var data: Seq[T]) extends Partition with Serializable{

  def iterator : Iterator[T] = data.iterator

  // same as ParallelCollectionPartition[T]
  override def hashCode() : Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowPartition[_] => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer

    // Treat java serializer with default action rather than going thru serialization, to avoid a
    // separate serialization header.

    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeLong(rddId)
        out.writeInt(slice)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser)(_.writeObject(data))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser)(ds => data = ds.readObject[Seq[T]]())
    }
  }
}

/* This class works the same as ParallelCollectionRDD, used in the sc.parallelize(Seq[T]) method
* and it uses Arrow-backed value vectors instead of the usual Scala Seq[T] collection */
class ArrowRDD[T: ClassTag](sc: SparkContext,
                            @transient private val data: ValueVector,
                            numSlices: Int,
                            locationPrefs: Map[Int, Seq[String]]) extends RDD[T](sc, Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, split.asInstanceOf[ArrowPartition[T]].iterator)
  }

  override protected def getPartitions: Array[Partition] = {
    val values = ArrowRDD.vectorExtractor(data)
    val slices = ArrowRDD.slice[T](values, numSlices).toArray
    slices.indices.map(i => new ArrowPartition(id, i, slices(i))).toArray
  }

  override def getPreferredLocations(s: Partition) : Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object ArrowRDD {

  def slice[T: ClassTag](seq: Seq[T],
                         numSlices: Int) : Seq[Seq[T]] = {

    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    def positions(length: Long, numSlices: Int) : Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end   = (((i + 1) * length) / numSlices).toInt

        (start,end)
      }
    }
    /**
     * Slice original ValueVector with zero-copy, using the Slicing operations described
     * in the Java Arrow API documentation page.
     * Then retrieve the data from each individual slice and convert it to Seq[T]
     */
//    positions(vec.getValueCount, numSlices).map {
//      case (start, end) => {
//        val allocator = vec.getAllocator
//        val tp        = vec.getTransferPair(allocator)
//
//        tp.splitAndTransfer(start, end)
//        val splitVector = tp.getTo
//
//        (0 until end).iterator.map {
//          i => splitVector.getObject(i).asInstanceOf[T]
//        }.toSeq
//      }
//    }.toSeq
//    val values = (0 until vec.getValueCount).iterator.map(i =>
//          vec.getObject(i).asInstanceOf[T]).toSeq
//
//
//    positions(values.length, numSlices).map {
//      case (start, end) => values.slice(start, end)
//    }.toSeq
//    vectorExtractor[T](vec)
//    positions(values.length, numSlices).map {
//      case (start, end) => values.slice(start, end)
//    }.toSeq
    seq match {
//      case r: Range =>
//        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
//          // If the range is inclusive, use inclusive range for the last slice
//          if (r.isInclusive && index == numSlices - 1) {
//            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
//          } else {
//            new Range.Inclusive(r.start + start * r.step, r.start + (end - 1) * r.step, r.step)
//          }
//        }.toSeq.asInstanceOf[Seq[Seq[T]]]
//      case nr: NumericRange[T] =>
//        // For ranges of Long, Double, BigInteger, etc
//        val slices = new ArrayBuffer[Seq[T]](numSlices)
//        var r = nr
//        for ((start, end) <- positions(nr.length, numSlices)) {
//          val sliceSize = end - start
//          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
//          r = r.drop(sliceSize)
//        }
//        slices.toSeq
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
          array.slice(start, end).toSeq
        }.toSeq
    }
  }

   def vectorExtractor[T: ClassTag](vector: ValueVector) : Seq[T] = {
    val vectorType = vector.getMinorType

    vectorType match {
      case MinorType.BIGINT => {
        (0 until vector.getValueCount).iterator.map {
          i => vector.asInstanceOf[BigIntVector].get(i).asInstanceOf[T]
        }.toSeq
      }
      case MinorType.VARBINARY => {
        (0 until vector.getValueCount).iterator.map {
          i => {
            val dataBuf = vector.asInstanceOf[BaseVariableWidthVector].getDataBuffer
            val offBuf = vector.asInstanceOf[BaseVariableWidthVector].getOffsetBuffer
            BaseVariableWidthVector.get(dataBuf, offBuf, i).asInstanceOf[T]
          }
        }.toSeq
      }
      case _ => throw new Exception("Unsupported minor type")
    }
  }

}

