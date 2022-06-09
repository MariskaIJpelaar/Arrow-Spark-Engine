package org.apache.spark.sql

package object column {
  type ColumnDataFrame[T] = Dataset[TColumn[T]]
}
