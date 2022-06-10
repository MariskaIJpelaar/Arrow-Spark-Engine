package org.apache.spark.rdd.sparrow

import org.apache.arrow.vector.ValueVector
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Externalizable, ObjectInput, ObjectInputStream, ObjectOutput, ObjectOutputStream}

/** Wrapper to trick spark into understanding our ArrowPartition :)
 * Note: we do not actually implement a lot, because we are not actually a row */
case class ArrowInternalColumn(private var partition: ArrowPartition) extends InternalRow with Externalizable {
  /** No-arg constructor for (de-)serialization */
  def this() = this(new ArrowPartition())

  /** Note: helper function to copy data of partitions */
  def transfer(): Array[ValueVector] = {
    partition._data.map { vector =>
      val allocator = vector.getAllocator
      val tp = vector.getTransferPair(allocator)

      tp.transfer()
      tp.getTo
    }
  }


  override def numFields: Int = partition.getLen

  override def update(i: Int, value: Any): Unit = {
    throw new UnsupportedOperationException()
  }

  override def setNullAt(i: Int): Unit = update(i, null)

  override def isNullAt(ordinal: Int): Boolean = {
    throw new UnsupportedOperationException()
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    throw new UnsupportedOperationException()
  }

  override def getByte(ordinal: Int): Byte = {
    throw new UnsupportedOperationException()
  }

  override def getShort(ordinal: Int): Short = {
    throw new UnsupportedOperationException()
  }

  override def getInt(ordinal: Int): Int = {
    throw new UnsupportedOperationException()
  }

  override def getBoolean(ordinal: Int): Boolean = {
    throw new UnsupportedOperationException()
  }

  override def getLong(ordinal: Int): Long = {
    throw new UnsupportedOperationException()
  }

  override def getFloat(ordinal: Int): Float = {
    throw new UnsupportedOperationException()
  }

  override def getDouble(ordinal: Int): Double = {
    throw new UnsupportedOperationException()
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    throw new UnsupportedOperationException()
  }

  override def copy(): InternalRow = {
    ArrowInternalColumn(new ArrowPartition(partition._rddId, partition._slice, transfer()))
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    throw new UnsupportedOperationException()
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    throw new UnsupportedOperationException()
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    throw new UnsupportedOperationException()
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    throw new UnsupportedOperationException()
  }

  override def getArray(ordinal: Int): ArrayData = {
    throw new UnsupportedOperationException()
  }

  override def getMap(ordinal: Int): MapData = {
    throw new UnsupportedOperationException()
  }

  override def writeExternal(objectOutput: ObjectOutput): Unit = { partition.writeExternal(objectOutput) }
  override def readExternal(objectInput: ObjectInput): Unit = { partition.readExternal(objectInput)  }
}

object ArrowInternalColumn {
  /**  Note: similar to getByteArrayRdd(...) */
  def encode(n: Int, iter: Iterator[ArrowInternalColumn]): Iterator[(Long, Array[Byte])] = {
    var count: Long = 0
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(codec.compressedOutputStream(bos))

    while ((n < 0 || count < n) && iter.hasNext) {
      oos.writeInt(1)
      iter.next().writeExternal(oos)
      count += 1
    }

    oos.writeInt(0)
    oos.flush()
    oos.close()
    Iterator((count, bos.toByteArray))
  }

  /** Note: similar to decodeUnsafeRows */
  def decode(bytes: Array[Byte]): Iterator[ArrowInternalColumn] = {
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(codec.compressedInputStream(bis))

    new NextIterator[ArrowInternalColumn] {
      override protected def getNext(): ArrowInternalColumn = {
        if (ois.readInt() == 0) {
          finished = true
          return null
        }
        val wrapper = new ArrowInternalColumn(new ArrowPartition())
        wrapper.readExternal(ois)
        wrapper
      }

      override protected def close(): Unit = ois.close()
    }
  }
}
