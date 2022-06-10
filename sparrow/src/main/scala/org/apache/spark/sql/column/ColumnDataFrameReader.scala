package org.apache.spark.sql.column

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrameReader, SparkSession}

import java.util.Locale

/** Used to extend DataFrameReader with load functionalities to create
 * Dataset[TColumn] */
class ColumnDataFrameReader(sparkSession: SparkSession) extends DataFrameReader(sparkSession) {

  // TODO: implement
 /** Loads input in as ColumnDataFrame, for data sources that don't require a path */
 def load[T](): ColumnDataFrame[T] = load(Seq.empty: _*)
 /** Loads input in as a ColumnDataFrame, for data sources that require a path */
 def load[T](path: String): ColumnDataFrame[T] = {
   if (sparkSession.sessionState.conf.legacyPathOptionBehavior) {
    option("path", path).asInstanceOf[ColumnDataFrameReader].load(Seq.empty: _*)
   } else {
    load(Seq(path): _*)
   }
 }
 /** Loads input in as a DataFrame, for data sources with multiple paths
  * Only works if the source is an HadoopFsRelationProvider */
 def load[T](paths: String*): ColumnDataFrame[T] = {
  if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER)
   throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")

  val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
  if (!legacyPathOptionBehavior &&
    (extraOptions.contains("path") || extraOptions.contains("paths")) && paths.nonEmpty)
   throw QueryCompilationErrors.pathOptionNotSetCorrectlyWhenReadingError()

  DataSource.lookupDataSourceV2(source, sparkSession.sessionState.conf).flatMap { provider =>
  // TODO: implement

    Column
  }
 }

 /** Functions to get the private members of DataFrameReader */
 private def getPrivate[T](name: String): T = {
  val field = classOf[DataFrameReader].getDeclaredField(name)
  field.setAccessible(true)
  field.get(this).asInstanceOf[T]
 }
 protected def source: String = getPrivate("source")
 protected def userSpecifiedSchema: Option[StructType] = getPrivate("userSpecifiedSchema")
 protected def extraOptions : CaseInsensitiveMap[String] = getPrivate("extraOptions")
}
