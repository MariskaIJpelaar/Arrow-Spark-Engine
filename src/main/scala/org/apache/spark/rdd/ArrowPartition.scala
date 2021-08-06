package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.{BigIntVector, ValueVector, VarBinaryVector, ZeroVector}
import org.apache.spark.{Partition, SparkException}
import org.apache.spark.internal.Logging

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import scala.reflect.ClassTag

class ArrowPartition extends Partition with Externalizable with Logging {

  private var _rddId : Long = 0L
  private var _slice : Int = 0
  private var _data : ValueVector = new ZeroVector

  def this(rddId : Long, slice : Int, data : ValueVector) = {
    this()
    _rddId = rddId
    _slice = slice
    _data = data
  }

  def iterator[T: ClassTag] : Iterator[T] = {
    var idx = 0

    new Iterator[T] {
      override def hasNext: Boolean = idx < _data.getValueCount

      override def next(): T = {
        val value = _data.getObject(idx).asInstanceOf[T]
        idx += 1

        value
      }
    }
  }

  override def hashCode() : Int = (43 * (43 + _rddId) + _slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowPartition => this._rddId == that._rddId && this._slice == that._slice
    case _ => false
  }

  override def index: Int = _slice

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(_rddId)
    out.writeInt(_slice)

    val len = _data.getValueCount
    out.writeInt(len)
    val vecType = _data.getMinorType
    out.writeObject(vecType)
    for (i <- 0 until len) out.writeObject(_data.getObject(i))
  }

  override def readExternal(in: ObjectInput): Unit = {
    _rddId = in.readLong()
    _slice = in.readInt()

    val len = in.readInt()
    val vecType = in.readObject().asInstanceOf[MinorType]
    val allocator = new RootAllocator(Long.MaxValue)

    vecType match {
      case MinorType.BIGINT =>
        _data = new BigIntVector("vector", allocator)
        _data.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len) _data.asInstanceOf[BigIntVector]
          .setSafe(i, in.readObject().asInstanceOf[Long])
        _data.setValueCount(len)
      case MinorType.VARBINARY =>
        _data = new VarBinaryVector("vector", allocator)
        _data.asInstanceOf[VarBinaryVector].allocateNew(len)
        for (i <- 0 until len) _data.asInstanceOf[VarBinaryVector]
          .setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
        _data.setValueCount(len)
      case _ => throw new SparkException("Unsupported Arrow Vector")
    }
  }
}
