package org.apache.arrow.util.vector.read

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{ValueVector, VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema

/** Fixme: test while integrating into arrow-spark */
class ArrowReaderIterator(protected val delegate: Iterator[ArrowRecordBatch], protected val schema: Schema) extends Iterator[Array[ValueVector]] {
  protected val rootAllocator = new RootAllocator(Integer.MAX_VALUE)

  override def hasNext: Boolean = delegate.hasNext

  override def next(): Array[ValueVector] = {
    val batch: ArrowRecordBatch = delegate.next()
    val root = VectorSchemaRoot.create(schema, rootAllocator)
    new VectorLoader(root).load(batch)
    root.getFieldVectors.asInstanceOf[Seq[ValueVector]].toArray
  }
}

