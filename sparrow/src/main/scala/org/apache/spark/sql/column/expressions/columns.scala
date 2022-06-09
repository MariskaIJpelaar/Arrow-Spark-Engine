package org.apache.spark.sql.column.expressions

import org.apache.spark.sql.column.TColumn


class GenericColumn[T](protected[sql] val values: Array[T]) extends TColumn[T] {
  /** No-arg constructor for serialization */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[T](size))

  /** Number of elements in the Column */
  override def length: Int = values.length

  /** Returns the value at position i. If the value is null, null is returned */
  override def get(i: Int): Option[T] = {
    if (i < 0 || i >= length) return None
    Some(values(i))
  }

  /** Make a copy of the current Column object */
  override def copy(): TColumn[T] = this
}
