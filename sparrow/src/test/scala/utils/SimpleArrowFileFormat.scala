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

  private var schema: Option[Schema] = None

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    var parquetSchema: Option[MessageType] = None
    val filepaths = files.map( status => status.getPath )
    filepaths.foreach( path => {
      try {
        val inputFile = HadoopInputFile.fromPath(path, new Configuration())
        val reader = ParquetFileReader.open(inputFile)
        if (parquetSchema.isEmpty) parquetSchema = Some(reader.getFileMetaData.getSchema)
        else parquetSchema.get.union(reader.getFileMetaData.getSchema)
      } catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    })
    if (parquetSchema.isDefined) {
      val converter: SchemaConverter = new SchemaConverter()
      val mapping: SchemaMapping = converter.fromParquet(parquetSchema.get)
      schema = Some(mapping.getArrowSchema)
    }

    // TODO: get StructType
  }

  // TODO: can we skip?
  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = ???

  override def shortName(): String = "sparrow"
  override def toString: String = "SpArrow"

  /** Returns a function that can be used to read a single file in as an Iterator of Array[ValueVector] */
  override def buildArrowReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedArrowFile => Iterator[Array[ValueVector]] = {
    // TODO: implement
    /** Note: we assume schema is set here */
    val root = VectorSchemaRoot.create(schema.get, new RootAllocator(Integer.MAX_VALUE))
    (file: PartitionedArrowFile) => {
      // TODO: per-file schema?
      val decodedpath = URLDecoder.decode(file.filePath, "UTF-8")
      val inputFile = HadoopInputFile.fromPath(new Path(decodedpath), new Configuration())
      val reader = ParquetFileReader.open(inputFile)
      // TODO: per-file root?
      // TODO: set iterator
      val itr: Iterator[ArrowRecordBatch] = null
      itr.foreach( batch => new VectorLoader(root).load(batch))
      root.getFieldVectors.asInstanceOf[List[ValueVector]].toArray
    }
  }
}
