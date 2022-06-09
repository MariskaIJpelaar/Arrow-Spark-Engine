package org.apache.spark.sql.column

import org.apache.spark.sql.column.expressions.GenericColumn
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object TColumn {
  /** This method can be used to extract values from a TColumn object */
  def unapplySeq[T](col: TColumn[T]): Seq[Option[T]] = col.toSeq

  /** This method can be used to construct a TColumn with the given values */
  def apply[T](values: T*): TColumn[T] = new GenericColumn[T](values.toArray)

  /** This method can be used to construct a TColumn from a Seq of values */
  def fromSeq[T](values: Seq[T]): TColumn[T] = new GenericColumn[T](values.toArray)

  /** Returns an empty TColumn */
  val empty: TColumn[Nothing] = apply()
}

/** Note: inspiration from: Row.scala */
/** Note: T stands for Trait ;)
 * Unfortunately, spark already had a Column class*/
trait TColumn[T] extends Serializable {
  /** Number of elements in the Column */
  def size: Int = length

  /** Number of elements in the Column */
  def length: Int

  /** Returns the value at position i. If the value is null, null is returned. */
  def apply(i: Int): Option[T] = get(i)

  /** Returns the value at position i. If the value is null, null is returned */
  def get(i: Int): Option[T]

  /** Checks whether the value at position i is null */
  def isNullAt(i: Int): Boolean = get(i).isDefined

  override def toString: String = this.mkString("[", ",", "]")

  /** Make a copy of the current Column object */
  def copy(): TColumn[T]

  /** Returns true if there are any NULL values in this row */
  def anyNull: Boolean = 0 until length exists (i => isNullAt(i))

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[TColumn[T]]) return false
    val other = o.asInstanceOf[TColumn[T]]

    if (other eq null) return false
    if (length != other.length) return false
    (0 until length) forall (i => get(i) == other.get(i))
  }

  override def hashCode(): Int = {
    // Using Scala's Seq hash code implementation
    var h = MurmurHash3.seqSeed
    0 until length foreach( i => h = MurmurHash3.mix(h, apply(i).##))
    MurmurHash3.finalizeHash(h, length)
  }

  /* ---------------------- utility methods for Scala ---------------------- */
  /** Return a Scala Seq representing the Column.
   *  Elements are placed in the same order in the Seq */
  def toSeq: Seq[Option[T]] = Seq.tabulate(length)( i => get(i) )

  /** Displays all elements of this sequence in a string */
  def mkString: String = mkString("")

  /** Displays all elements of this sequence in a string using a separator string */
  def mkString(sep: String): String = mkString("", sep, "")

  /** Displays all elements of this sequence in a string
   * using start, end, and separator string */
  def mkString(start: String, sep: String, end: String): String = {
    val builder = new mutable.StringBuilder(start)
    if (length > 0) builder.append(get(0))
    1 until length foreach { i =>
      builder.append(sep)
      builder.append(get(i))
    }
    builder.append(end)
    builder.toString()
  }



  /** Note: perhaps in the future, json functionalities will be supported here */
}
