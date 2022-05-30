package utils

import org.apache.arrow.vector.ValueVector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ArrowFileFormat
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedArrowFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

/** SimpleArrowFileFormat that does not support filters or options
 * Note: some functions have been copied from:
 * https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow-spark/blob/master/arrow-spark-connector/src/main/scala/org/apache/spark/sql/execution/datasources/arrow/ArrowFileFormat.scala */
class SimpleArrowFileFormat extends ArrowFileFormat with DataSourceRegister with Serializable with Logging {
  /** Checks whether we can split the file: copied from arrow-spark::ArrowFileFormat */
  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = false

  // TODO: variable for Schema as used in ParquetReader?

  // TODO: implement
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = ???

  // TODO: can we skip?
  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = ???

  override def shortName(): String = "sparrow"
  override def toString: String = "SpArrow"

  /** Returns a function that can be used to read a single file in as an Iterator of Array[ValueVector] */
  override def buildArrowReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedArrowFile => Iterator[Array[ValueVector]] = {
    // TODO: implement
  }
}
