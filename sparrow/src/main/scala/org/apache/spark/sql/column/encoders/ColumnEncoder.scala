package org.apache.spark.sql.column.encoders

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, If, IsNull, Literal}
import org.apache.spark.sql.column.TColumn
import org.apache.spark.sql.column.expressions.objects.{CreateExternalColumn, GetExternalColumn}
import org.apache.spark.sql.types.ObjectType

import scala.reflect.ClassTag

object ColumnEncoder {
 // TODO: create apply functions

  def apply[T](): ExpressionEncoder[TColumn[T]] = {
    val cls = classOf[TColumn[T]]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val serializer = serializerFor(inputObject)
    // TODO: val deserializer = deserializerFor(GetColumnByOrdinal(0, serializer.dataType), schema)
    val deserializer = deserializerFor()
    new ExpressionEncoder[TColumn[T]](serializer, deserializer, ClassTag(cls))
  }

  /** Note: as we only use serialzeFor(...) for StructTypes, we shall omit all other types, and implement only when needed
   * Same holds for nullability*/
  private def serializerFor(inputObject: Expression): Expression =  {
    val nonNullOutput = GetExternalColumn(inputObject)
    expressionForNullableExpr(inputObject, nonNullOutput)
  }

  /** Note below methods are copied from RowEncoder */
  private def deserializerFor(input: Expression): Expression = {
    val seq = Seq(input): _* // won't work if we do not make a separate variable?
    CreateExternalColumn(seq)
  }

  private def expressionForNullableExpr(
   expr: Expression,
   newExprWhenNotNull: Expression): Expression = {
    If(IsNull(expr), Literal.create(null, newExprWhenNotNull.dataType), newExprWhenNotNull)
  }
}
