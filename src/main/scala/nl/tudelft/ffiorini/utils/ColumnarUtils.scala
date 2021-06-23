package nl.tudelft.ffiorini.utils

import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.reflect.ClassTag
import org.apache.arrow.vector.{BaseVariableWidthVector, BigIntVector, ValueVector, VectorSchemaRoot}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class ColumnarUtils {

  def toIterator(schema: VectorSchemaRoot): Iterator[Long] = {
    val v = getIntegerVector(schema).get
    var idx = 0
    var value = 0L

    new Iterator[Long] {
      override def hasNext: Boolean = idx >= 0 && idx < v.getValueCount

      override def next(): Long = {
        value = v.get(idx)
        idx += 1

        value
      }
    }
  }

  def getIntegerVector(schema: VectorSchemaRoot): Option[BigIntVector] = {
    val allVectors = schema.getFieldVectors.asScala
    for (v <- allVectors) {
      if (v.getField.getType.getTypeID.equals(ArrowType.ArrowTypeID.Int)) {
        v.asInstanceOf[BigIntVector]
      }
    }
    Option.empty[BigIntVector]
  }
}


