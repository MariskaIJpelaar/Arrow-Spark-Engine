package utils

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{ValueVector, VectorLoader, VectorSchemaRoot}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ArrowFileFormat
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedArrowFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.parquet.arrow.schema.{SchemaConverter, SchemaMapping}

import java.io.IOException
import java.net.URLDecoder

/** SimpleArrowFileFormat that does not support filters or options
 * Note: some functions have been copied from:
 * https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow-spark/blob/master/arrow-spark-connector/src/main/scala/org/apache/spark/sql/execution/datasources/arrow/ArrowFileFormat.scala */
class SimpleArrowFileFormat extends ArrowFileFormat with DataSourceRegister with Serializable with Logging {
  /** Checks whether we can split the file: copied from arrow-spark::ArrowFileFormat */
  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = false

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    // TODO: get StructType
  }

  // TODO: can we skip?
  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = ???

  override def shortName(): String = "sparrow"
  override def toString: String = "SpArrow"

  /** Returns a function that can be used to read a single file in as an Iterator of Array[ValueVector] */
  override def buildArrowReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedArrowFile => Iterator[Array[ValueVector]] = {
    (file: PartitionedArrowFile) => {
      val inputFile = HadoopInputFile.fromPath(new Path(URLDecoder.decode(file.filePath, "UTF-8")), new Configuration())
      val parquetSchema = ParquetFileReader.open(inputFile).getFileMetaData.getSchema
      val converter: SchemaConverter = new SchemaConverter()
      val schema = converter.fromParquet(parquetSchema)
      // TODO: get iterator
    }
  }
}
