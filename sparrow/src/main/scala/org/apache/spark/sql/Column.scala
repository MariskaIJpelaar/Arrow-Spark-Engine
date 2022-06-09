package org.apache.spark.sql

import org.json4s.JValue

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/** Note: inspiration from: Row.scala */
trait Column[T] extends Serializable {
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
  def copy(): Column[T]

  /** Returns true if there are any NULL values in this row */
  def anyNull: Boolean = 0 until length exists (i => isNullAt(i))

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[Column[T]]) return false
    val other = o.asInstanceOf[Column[T]]

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

  /** The compact JSON representation of this Column */
  def json: String = compact(jsonValue)

  /** The pretty (i.e. indented) JSON representation of this Column */
  def prettyJson: String = pretty(render(jsonValue))

  /** JSON representation of the Column */
  private[sql] def jsonValue: JValue = {
    // TODO: implement
  }

  // TODO: implement other functions
}
