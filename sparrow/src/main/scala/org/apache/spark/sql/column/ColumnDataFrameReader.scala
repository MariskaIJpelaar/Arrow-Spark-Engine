package org.apache.spark.sql.column

import org.apache.spark.sql.DataFrameReader

/** Used to extend DataFrameReader with load functionalities to create
 * Dataset[TColumn] */
class ColumnDataFrameReader(private val reader: DataFrameReader)  {
  // TODO: implement
 /** Loads input in as ColumnDataFrame, for data sources that don't require a path */
 def load[T](): ColumnDataFrame[T] = ???
 /** Loads input in as a ColumnDataFrame, for data sources that require a path */
 def load[T](path: String): ColumnDataFrame[T] = ???
 /** Loads input in as a DataFrame, for data sources with multiple paths
  * Only works if the source is an HadoopFsRelationProvider */
 def load[T](paths: String*): ColumnDataFrame[T] = ???
}
