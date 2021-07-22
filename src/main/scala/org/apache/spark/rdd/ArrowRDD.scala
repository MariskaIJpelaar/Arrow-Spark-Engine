package org.apache.spark.rdd

import org.apache.arrow
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
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
private [spark] class ArrowPartition[T: ClassTag] extends Partition with Externalizable{

  var rddId: Long = 0L
  var slice : Int  = 0
  var data : BigIntVector = new BigIntVector("vector", new RootAllocator(Long.MaxValue))

  /*Hack: default (primary) constructor takes zero arguments to be used for de-/serialization
  * using the Externalizable interface.
  * This 3-args constructor is used for the operations in ArrowRDD */
  def this(rddId : Long, slice : Int, data : BigIntVector) = {
    this()
    this.rddId = rddId
    this.slice = slice
    this.data = data
  }

  def iterator : Iterator[T] = {
    var idx = 0

    new Iterator[T] {
      override def hasNext: Boolean = idx < data.getValueCount

      override def next(): T = {
//        val vecType = data.getMinorType
//
//        vecType match {
//          case MinorType.BIGINT =>
//            val value = data.asInstanceOf[BigIntVector].get(idx)
//            idx = idx + 1
//
//            value.asInstanceOf[T]
//          case MinorType.VARBINARY =>
//            val dataBuf = data.asInstanceOf[BaseVariableWidthVector].getDataBuffer
//            val offBuf = data.asInstanceOf[BaseVariableWidthVector].getOffsetBuffer
//            val value = BaseVariableWidthVector.get(dataBuf, offBuf, idx)
//            idx = idx + 1
//
//            value.asInstanceOf[T]
//          case _ => throw new Exception("Unsupported vector type")
//        }
        val value = data.get(idx)
        idx += 1

        value.asInstanceOf[T]
      }
    }
  }

  // same as ParallelCollectionPartition[T]
  override def hashCode() : Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowPartition[_] => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  @throws(classOf[IOException])
  override def writeExternal(out: ObjectOutput): Unit = {
    println("Begin writeExternal")
//    out.writeLong(rddId)
//    out.writeInt(slice)
//    out.writeObject(data.getMinorType)
//    for (i <- 0 until data.getValueCount-1) {
//      out.writeObject(data.getObject(i))
//    }
    out.writeLong(rddId)
    out.writeInt(slice)
    for (i <- 0 until data.getValueCount-1)
      out.writeLong(data.get(i))
    println("End writeExternal")
  }

  override def readExternal(in: ObjectInput): Unit = {
    println("Begin readExternal")
//    rddId = in.readLong()
//    slice = in.readInt()
//    val minorType = in.readObject().asInstanceOf[MinorType]
//
//    val allocator : BufferAllocator = new RootAllocator(Long.MaxValue)
//    minorType match {
//      case MinorType.BIGINT => {
//        var vector = new BigIntVector("vector", allocator)
//        var index = 0
//        while (in.available() > 0){
//          vector.setSafe(index, in.readLong())
//          index+=1
//        }
//
//        data = vector.asInstanceOf[ValueVector]
//      }
//      case MinorType.VARBINARY => {
//        var vector = new VarCharVector("vector", allocator)
//        var index = 0
//        while (in.available() > 0){
//          vector.setSafe(index, in.readObject().asInstanceOf[Array[Byte]])
//          index+=1
//        }
//
//        data = vector.asInstanceOf[ValueVector]
//      }
//      case _ => throw new InvalidClassException("No valid type")
//    }
    rddId = in.readLong()
    slice = in.readInt()
    val allocator = new RootAllocator(Long.MaxValue)
    data = new BigIntVector("vector", allocator)
    var idx = 0
    while (in.available() > 0){
      data.setSafe(idx, in.readLong())
      idx += 1
    }
    println("End readExternal")
  }
}

/* This class works the same as ParallelCollectionRDD, used in the sc.parallelize(Seq[T]) method
* and it uses Arrow-backed value vectors instead of the usual Scala Seq[T] collection */
private [spark] class ArrowRDD[T: ClassTag](sc: SparkContext,
                            @transient private val data: BigIntVector,
                            numSlices: Int,
                            locationPrefs: Map[Int, Seq[String]]) extends RDD[T](sc, Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, split.asInstanceOf[ArrowPartition[T]].iterator)
  }

  override protected def getPartitions: Array[Partition] = {
    val slices = ArrowRDD.slice[T](data, numSlices).toArray

    slices.indices.map(i => new ArrowPartition(id, i, slices(i))).toArray
  }

  override def getPreferredLocations(s: Partition) : Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object ArrowRDD {

  def slice[T: ClassTag](vec: BigIntVector,
                         numSlices: Int): Seq[BigIntVector] = {

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
     * Then retrieve the data from each individual slice and convert it to Seq[T]
     */
    //    positions(vec.getValueCount, numSlices).map {
    //      case (start, end) => {
    //        val allocator = vec.getAllocator
    //        val tp = vec.getTransferPair(allocator)
    //
    //        tp.splitAndTransfer(start, end)
    //        val splitVector = tp.getTo
    //
    //        (0 until end).iterator.map {
    //          i => splitVector.getObject(i).asInstanceOf[T]
    //        }.toSeq
    //      }
    //    }
    positions(vec.getValueCount, numSlices).map {
      case (start, end) => {
        val allocator = vec.getAllocator
        val tp = vec.getTransferPair(allocator)

        println(start +" " +end+ " indexes. Count: " +vec.getValueCount)
        tp.splitAndTransfer(start, end-start)
        tp.getTo.asInstanceOf[BigIntVector]
      }
    }.toSeq
  }
}

