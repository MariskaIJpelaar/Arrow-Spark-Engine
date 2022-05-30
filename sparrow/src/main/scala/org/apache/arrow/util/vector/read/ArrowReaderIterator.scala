package org.apache.arrow.util.vector.read

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema

class ArrowReaderIterator(protected val delegate: Iterator[ArrowRecordBatch], protected val schema: Schema) extends Iterator[Array[ValueVector]] {
  protected val rootAllocator = new RootAllocator(Integer.MAX_VALUE)

  override def hasNext: Boolean = delegate.hasNext

  override def next(): Array[ValueVector] = {
    val batch: ArrowRecordBatch = delegate.next()
    val root = VectorSchemaRoot.create(schema, rootAllocator)
    //TODO: implement
  }
}
