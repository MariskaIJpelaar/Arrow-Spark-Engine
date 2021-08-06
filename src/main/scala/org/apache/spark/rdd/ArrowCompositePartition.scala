package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.{BigIntVector, ValueVector, VarBinaryVector, ZeroVector}
import org.apache.spark.Partition
import org.apache.spark.internal.Logging

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import scala.reflect.ClassTag

class ArrowCompositePartition extends Partition with Externalizable with Logging {

  var rddId: Long = 0L
  var slice: Int = 0
  var data: (ValueVector, ValueVector) = (new ZeroVector, new ZeroVector)

  def this(rddId: Long, slice: Int, data: (ValueVector, ValueVector)) = {
    this()
    this.rddId = rddId
    this.slice = slice
    this.data = data
  }

  def iterator[T: ClassTag, U: ClassTag] : Iterator[(T,U)] = {
    require(data._1.getValueCount == data._2.getValueCount,
    "Both vectors have to be equal in size")

    var idx = 0

    new Iterator[(T, U)] {
      //Since v1 and v2 have to be the same length, only check once
      override def hasNext: Boolean = idx < data._1.getValueCount

      override def next(): (T, U) = {
        val value1 = data._1.getObject(idx).asInstanceOf[T]
        val value2 = data._2.getObject(idx).asInstanceOf[U]
        idx += 1

        (value1, value2)
      }
    }
  }

  override def hashCode(): Int = (47 * (47 + rddId) + slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowCompositePartition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(rddId)
    out.writeInt(slice)

    val len = data._1.getValueCount //same as above...
    val minorType1 = data._1.getMinorType
    val minorType2 = data._2.getMinorType

    out.writeInt(len)
    out.writeObject((minorType1, minorType2)) //pass it as tuple (easier reading?)

    //write vector1 first, then vector2 (easier reading?)
    for (i <- 0 until len) {
      out.writeObject(data._1.getObject(i))
      out.writeObject(data._2.getObject(i))
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    rddId = in.readLong()
    slice = in.readInt()

    val len = in.readInt()
    val allocator = new RootAllocator(Long.MaxValue)
    val minorType = in.readObject().asInstanceOf[(MinorType, MinorType)]

    minorType match {
      case (MinorType.BIGINT, MinorType.BIGINT) =>
        data = (new BigIntVector("vector1", allocator), new BigIntVector("vector2", allocator))
        data._1.asInstanceOf[BigIntVector].allocateNew(len)
        data._2.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len){
          data._1.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
          data._2.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
        }
        data._1.setValueCount(len)
        data._2.setValueCount(len)
      case (MinorType.BIGINT, MinorType.VARBINARY) =>
        data = (new BigIntVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
        data._1.asInstanceOf[BigIntVector].allocateNew(len)
        data._2.asInstanceOf[VarBinaryVector].allocateNew(len)
        for (i <- 0 until len){
          data._1.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
          data._2.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
        }
        data._1.setValueCount(len)
        data._2.setValueCount(len)
      case (MinorType.VARBINARY, MinorType.BIGINT) =>
        data = (new VarBinaryVector("vector1", allocator), new BigIntVector("vector2", allocator))
        data._1.asInstanceOf[VarBinaryVector].allocateNew(len)
        data._2.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len){
          data._1.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
          data._2.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
        }
        data._1.setValueCount(len)
        data._2.setValueCount(len)
      case (MinorType.VARBINARY, MinorType.VARBINARY) =>
        data = (new VarBinaryVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
        data._1.asInstanceOf[VarBinaryVector].allocateNew(len)
        data._2.asInstanceOf[VarBinaryVector].allocateNew(len)
        for (i <- 0 until len){
          data._1.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
          data._2.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
        }
        data._1.setValueCount(len)
        data._2.setValueCount(len)
      case _ => throw new Exception("Unsupported Arrow vector(s)")
    }
  }
}

class ArrowHybridPartition extends Partition with Externalizable with Logging{
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = ???

  override def writeExternal(out: ObjectOutput): Unit = ???

  override def readExternal(in: ObjectInput): Unit = ???
}