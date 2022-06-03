package org.apache.spark.rdd

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/** Wrapper to trick spark into understanding our ArrowPartition :)
 * Note: we do not actually implement a lot, because we are not actually a row */
// TODO: do
case class ArrowWrapper(private var partition: ArrowPartition) extends InternalRow {
  override def numFields: Int = partition.getLen

  override def update(i: Int, value: Any): Unit = { throw UnsupportedOperationException }

  override def setNullAt(i: Int): Unit = update(i, null)

  override def isNullAt(ordinal: Int): Boolean = { throw UnsupportedOperationException }

  override def get(ordinal: Int, dataType: DataType): AnyRef = { throw UnsupportedOperationException }

  override def getByte(ordinal: Int): Byte = ???

  override def getShort(ordinal: Int): Short = ???

  override def getInt(ordinal: Int): Int = ???

  override def getBoolean(ordinal: Int): Boolean = ???

  override def getLong(ordinal: Int): Long = ???

  override def getFloat(ordinal: Int): Float = ???

  override def getDouble(ordinal: Int): Double = ???

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???

  override def copy(): InternalRow = ???

  override def getUTF8String(ordinal: Int): UTF8String = ???

  override def getBinary(ordinal: Int): Array[Byte] = ???

  override def getInterval(ordinal: Int): CalendarInterval = ???

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???

  override def getArray(ordinal: Int): ArrayData = ???

  override def getMap(ordinal: Int): MapData = ???
}
