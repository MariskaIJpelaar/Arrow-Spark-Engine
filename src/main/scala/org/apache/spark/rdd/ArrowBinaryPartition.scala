package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BaseVariableWidthVector, BigIntVector, VarBinaryVector}
import org.apache.spark.Partition
import org.apache.spark.util.Utils

import java.io.{Externalizable, ObjectInput, ObjectOutput}

/**
 * This class works the same as ParallelCollectionPartition, used in the ParallelCollectionRDD
 * and it uses Arrow-backed value vectors instead of the usual Scala Seq[T] collection.
 *
 * This is the specific case of BaseVariableWidth-like vectors, such as VarChar and VarBinary vectors.
 *
 */
class ArrowBinaryPartition extends Partition with Externalizable {

  var rddId : Long = 0L
  var slice: Int = 0
  var data : VarBinaryVector = new VarBinaryVector("vector", new RootAllocator(Long.MaxValue))

  /* Default constructor holds no arguments to allow Externalizable to work.
  * This 3-args constructor is used for operations in ArrowRDD */
  def this(rddId: Long, slice: Int, data: VarBinaryVector) = {
    this()
    this.rddId = rddId
    this.slice = slice
    this.data = data
  }

  def iterator : Iterator[Array[Byte]] = {
    var idx = 0

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = idx < data.getValueCount

      override def next(): Array[Byte] = {
        val value = data.get(idx)
        idx += 1

        value
      }
    }
  }

  override def hashCode() : Int = (43 * (43 + rddId) + slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowBinaryPartition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeLong(rddId)
    out.writeInt(slice)

    val len =  data.getValueCount
    out.writeInt(len)

    for (i <- 0 until data.getValueCount) out.writeObject(data.get(i))
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    rddId = in.readLong()
    slice = in.readInt()

    val allocator = new RootAllocator(Long.MaxValue)
    data = new VarBinaryVector("vector", allocator)

    val len = in.readInt()
    data.allocateNew(len)

    for (i <- 0 until len) {
      data.setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
    }
    data.setValueCount(len)
  }
}

/*This class works the same as ParallelCollectionPartition, used in the ParallelCollectionRDD below
* and it uses Arrow-backed value vectors instead of the usual Scala Seq[T] collection */
class ArrowLongPartition extends Partition with Externalizable{

  /* 26.07 HACK: specific case using BigIntVector (Long data type) */
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

  def iterator : Iterator[Long] = {
    var idx = 0

    new Iterator[Long] {
      override def hasNext: Boolean = idx < data.getValueCount

      override def next(): Long = {
        val value = data.get(idx)
        idx += 1

        value
      }
    }
  }

  // same as ParallelCollectionPartition[T]
  override def hashCode() : Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowLongPartition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeLong(rddId)
    out.writeInt(slice)

    /* Use valueCount to get exact amount of values in deserialized vector */
    val len = data.getValueCount
    out.writeInt(len)

    for (i <- 0 until data.getValueCount) {
      out.writeLong(data.get(i))
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    rddId = in.readLong()
    slice = in.readInt()

    val allocator = new RootAllocator(Long.MaxValue)
    data = new BigIntVector("vector", allocator)
    val len = in.readInt()
    data.allocateNew(len)

    for (i <- 0 until len) {
      data.setSafe(i, in.readLong())
    }
    data.setValueCount(len)
  }
}