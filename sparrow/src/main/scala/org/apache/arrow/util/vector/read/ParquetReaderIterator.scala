package org.apache.arrow.util.vector.read

import org.apache.arrow.vector.ValueVector
import org.apache.spark.sql.execution.datasources.PartitionedArrowFile

/** Abstract Base Parquet-Reader-Iterator Class to read parquet files
 * Implementations may support multiple types or single types */
abstract class ParquetReaderIterator(protected val file: PartitionedArrowFile) extends Iterator[Array[ValueVector]] {}

class IntegerParquetReaderIterator(protected val file: PartitionedArrowFile) extends Iterator[Array[ValueVector]] {

  // TODO: implement according to ParquetToArrowConverter.java

  override def hasNext: Boolean = ???

  override def next(): Array[ValueVector] = ???
}

