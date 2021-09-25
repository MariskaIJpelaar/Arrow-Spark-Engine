package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.{BigIntVector, IntVector, StringVector, ValueVector, VarBinaryVector, ZeroVector}
import org.apache.spark.{Partition, SparkException}
import org.apache.spark.internal.Logging

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import scala.reflect.runtime.universe._

/**
 * ArrowCompositePartition allows the use of Tuple2 amounts of ValueVectors as data input.
 * The logic is similar to the one in ArrowPartition, and this reasoning can be also applied to any
 * kind of tuple that can be defined in Scala (from Tuple2 to Tuple22).
 * For sake of simplicity and clarity, only this case has been implemented.
 *
 * Since the two vectors are required to be the same size, most likely only the first is checked in terms
 * of length and valueCount.
 */
class ArrowCompositePartition extends Partition with Externalizable with Logging {

  var _rddId: Long = 0L
  var _slice: Int = 0
  var _data: (ValueVector, ValueVector) = (new ZeroVector, new ZeroVector)

  def this(rddId: Long, slice: Int, data: (ValueVector, ValueVector)) = {
    this()
    this._rddId = rddId
    this._slice = slice
    this._data = data
  }

  def iterator[T: TypeTag, U: TypeTag] : Iterator[(T,U)] = {
    require(_data._1.getValueCount == _data._2.getValueCount,
    "Both vectors have to be equal in size")

    var idx = 0

    new Iterator[(T, U)] {
      //Since v1 and v2 have to be the same length, only check once
      override def hasNext: Boolean = idx < _data._1.getValueCount

      override def next(): (T, U) = {
        val value1 = _data._1.getObject(idx).asInstanceOf[T]
        val value2 = _data._2.getObject(idx).asInstanceOf[U]
        idx += 1

        (value1, value2)
      }
    }
  }

  override def hashCode(): Int = (47 * (47 + _rddId) + _slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowCompositePartition => this._rddId == that._rddId && this._slice == that._slice
    case _ => false
  }

  override def index: Int = _slice

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(_rddId)
    out.writeInt(_slice)

    val len = _data._1.getValueCount //same as above...
    val minorType1 = _data._1.getMinorType
    val minorType2 = _data._2.getMinorType

    out.writeInt(len)
    out.writeObject((minorType1, minorType2)) //pass it as tuple (easier reading?)

    //write vector1 first, then vector2 (easier reading?)
    for (i <- 0 until len) {
      out.writeObject(_data._1.getObject(i))
      out.writeObject(_data._2.getObject(i))
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    _rddId = in.readLong()
    _slice = in.readInt()

    val len = in.readInt()
    val allocator = new RootAllocator(Long.MaxValue)
    val minorType = in.readObject().asInstanceOf[(MinorType, MinorType)]

    /**
     * All cases for two ValueVectors have been considered (as said in ArrowPartition, BIGINT and
     * VARBINARY are used in this work to show the feasibility of this solution.
     *
     * A potential improvement on this solution could be removing one check for different types (i.e. a tuple
     * consisting of (BIGINT, VARBINARY) or (VARBINARY, BIGINT) could be considered the same, then swapped
     * properly when required.
     * Potential todo?
     */
    minorType match {
      case (MinorType.INT, MinorType.INT) =>
        _data = (new IntVector("vector1", allocator), new IntVector("vector2", allocator))
        _data._1.asInstanceOf[IntVector].allocateNew(len)
        _data._2.asInstanceOf[IntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
          _data._2.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.BIGINT, MinorType.BIGINT) =>
        _data = (new BigIntVector("vector1", allocator), new BigIntVector("vector2", allocator))
        _data._1.asInstanceOf[BigIntVector].allocateNew(len)
        _data._2.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
          _data._2.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.VARBINARY, MinorType.VARBINARY) =>
        _data = (new VarBinaryVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
        _data._1.asInstanceOf[VarBinaryVector].allocateNew(len)
        _data._2.asInstanceOf[VarBinaryVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
          _data._2.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.STRING, MinorType.STRING) =>
        _data = (new StringVector("vector1", allocator), new StringVector("vector2", allocator))
        _data._1.asInstanceOf[StringVector].allocateNew(len)
        _data._2.asInstanceOf[StringVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
          _data._2.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.INT, MinorType.BIGINT) =>
        _data = (new IntVector("vector1", allocator), new BigIntVector("vector2", allocator))
        _data._1.asInstanceOf[IntVector].allocateNew(len)
        _data._2.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
          _data._2.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.BIGINT, MinorType.INT) =>
        _data = (new BigIntVector("vector1", allocator), new IntVector("vector2", allocator))
        _data._1.asInstanceOf[BigIntVector].allocateNew(len)
        _data._2.asInstanceOf[IntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
          _data._2.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.INT, MinorType.VARBINARY) =>
        _data = (new IntVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
        _data._1.asInstanceOf[IntVector].allocateNew(len)
        _data._2.asInstanceOf[VarBinaryVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
          _data._2.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.VARBINARY, MinorType.INT) =>
        _data = (new VarBinaryVector("vector1", allocator), new IntVector("vector2", allocator))
        _data._1.asInstanceOf[VarBinaryVector].allocateNew(len)
        _data._2.asInstanceOf[IntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
          _data._2.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.INT, MinorType.STRING) =>
        _data = (new IntVector("vector1", allocator), new StringVector("vector2", allocator))
        _data._1.asInstanceOf[IntVector].allocateNew(len)
        _data._2.asInstanceOf[StringVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
          _data._2.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.STRING, MinorType.INT) =>
        _data = (new StringVector("vector1", allocator), new IntVector("vector2", allocator))
        _data._1.asInstanceOf[StringVector].allocateNew(len)
        _data._2.asInstanceOf[IntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
          _data._2.asInstanceOf[IntVector].setSafe(i, in.readObject().asInstanceOf[Int])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.BIGINT, MinorType.VARBINARY) =>
        _data = (new BigIntVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
        _data._1.asInstanceOf[BigIntVector].allocateNew(len)
        _data._2.asInstanceOf[VarBinaryVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
          _data._2.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.VARBINARY, MinorType.BIGINT) =>
        _data = (new VarBinaryVector("vector1", allocator), new BigIntVector("vector2", allocator))
        _data._1.asInstanceOf[VarBinaryVector].allocateNew(len)
        _data._2.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
          _data._2.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.BIGINT, MinorType.STRING) =>
        _data = (new BigIntVector("vector1", allocator), new StringVector("vector2", allocator))
        _data._1.asInstanceOf[BigIntVector].allocateNew(len)
        _data._2.asInstanceOf[StringVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
          _data._2.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case (MinorType.STRING, MinorType.BIGINT) =>
        _data = (new StringVector("vector1", allocator), new BigIntVector("vector2", allocator))
        _data._1.asInstanceOf[StringVector].allocateNew(len)
        _data._2.asInstanceOf[BigIntVector].allocateNew(len)
        for (i <- 0 until len){
          _data._1.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
          _data._2.asInstanceOf[BigIntVector].setSafe(i, in.readObject().asInstanceOf[Long])
        }
        _data._1.setValueCount(len)
        _data._2.setValueCount(len)
      case _ => throw new SparkException("Unsupported Arrow Vector(s)")
    }
  }
}