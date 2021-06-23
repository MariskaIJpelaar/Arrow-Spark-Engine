package nl.tudelft.ffiorini.utils

import org.apache.parquet.io.api.{GroupConverter, PrimitiveConverter}

/* Utility classes used in Animesh implementation. Moved to separate Scala file */
private class DumpConverter extends PrimitiveConverter {
  final override def asGroupConverter = new DumpGroupConverter
}

private class DumpGroupConverter extends GroupConverter {
  final def start() {}
  final def end() {}
  final def getConverter(fieldIndex: Int) = new DumpConverter
}

