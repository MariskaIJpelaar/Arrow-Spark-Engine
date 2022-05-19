/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.parquet;

import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.parquet.utils.DumpGroupConverter;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.network.protocol.Encoders;
import scala.collection.Iterator;
import scala.math.Numeric;
import scala.reflect.io.Directory;
import scala.reflect.io.File;

import java.io.IOException;
import java.util.*;

/***
 * This class provides the necessary logic to convert a Parquet File into
 * an usable Arrow data structure (in particular, a VectorSchemaRoot) and
 * populate it using the Parquet column values.
 *
 * The final aim for this class and the utilities is to be pushed into
 * Arrow codebase, in order to provide full Java support for reading
 * Parquet files without having to deal with native (JNI) code calls.
 */
public class ParquetToArrowConverter {
  private Configuration configuration;
  private RootAllocator allocator;
  private MessageType parquetSchema;
  public Schema arrowSchema;
  private VectorSchemaRoot vectorSchemaRoot;
  private List<Integer> rowsCount = null;

  private List<Double> time = new ArrayList<>();
  private Long t0, t1, t2, t3, t4, t5, t6, t7 = 0L;

  private scala.collection.immutable.List<File> files = null;
  private Iterator<File> it = null;

  public ParquetToArrowConverter() {
    t0 = System.nanoTime();
    this.configuration = new Configuration();
    this.allocator = new RootAllocator(Integer.MAX_VALUE);
    t1 = System.nanoTime();
    time.add((t1 - t0) / 1e9d);
  }

  public void clear() {
    parquetSchema = null;
    arrowSchema = null;
    vectorSchemaRoot.clear();
    vectorSchemaRoot = null;
    allocator.releaseBytes(allocator.getAllocatedMemory());
    allocator.close();
    configuration.clear();
    rowsCount.clear();
  }

  private void setVectors() {
    int n = vectorSchemaRoot.getFieldVectors().size();
    for (int i = 0; i < n; ++i) {
      vectorSchemaRoot.getVector(i).setValueCount(rowsCount.get(i));
    }
  }

  public void prepareDirectory(Directory dir) {
    files = dir.files().filter((file) -> Objects.equals(FilenameUtils.getExtension(file.name()), "parquet")).toList();
    it = files.toIterator();
  }

  /**
   * Processes all files as prepared by prepareDirectory(Directory)
   * Overwrites previous vectors
   * @return if an action was performed (there were still files left to process)
   */
  public boolean processFromDirectory() throws Exception {
    if (files == null)
      return false;
    if (!it.hasNext())
      return false;

    clear();
    process(it.next().toString());
    return true;
  }

  /**
   * Warning: only use this function if your data fits into memory
   * @param dir The Director to process
   */
  public void process(Directory dir) throws IOException {
    t2 = System.nanoTime();

    System.out.println("A" + PlatformDependent.usedDirectMemory());

    files = dir.files().filter((file) -> Objects.equals(FilenameUtils.getExtension(file.name()), "parquet")).toList();

    System.out.println("B" + PlatformDependent.usedDirectMemory());

    files.foreach( (file) -> {
      System.out.println("C" + PlatformDependent.usedDirectMemory());
      try {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(file.path()), configuration);
        System.out.println("CX" + PlatformDependent.usedDirectMemory());
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        System.out.println("CY" + PlatformDependent.usedDirectMemory());
        if (parquetSchema == null)
          parquetSchema = reader.getFileMetaData().getSchema();
        else
          parquetSchema.union(reader.getFileMetaData().getSchema());
        System.out.println("CZ" + PlatformDependent.usedDirectMemory());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return 0;
    });
    t3 = System.nanoTime();
    time.add((t3 - t2) / 1e9d);
    System.out.println("D" + PlatformDependent.usedDirectMemory());

    t4 = System.nanoTime();
    SchemaConverter converter = new SchemaConverter();
    System.out.println("E" + PlatformDependent.usedDirectMemory());
    SchemaMapping mapping = converter.fromParquet(parquetSchema);
    System.out.println("F" + PlatformDependent.usedDirectMemory());
    arrowSchema = mapping.getArrowSchema();
    System.out.println("G" + PlatformDependent.usedDirectMemory());
    vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
    System.out.println("H" + PlatformDependent.usedDirectMemory());
    rowsCount = new ArrayList<>(Collections.nCopies(vectorSchemaRoot.getFieldVectors().size(), 0));
    t5 = System.nanoTime();
    time.add((t5 - t4) / 1e9d);

    final int[] offset = {0}; // Array because it is passed by reference, not by value
    int totalRows = files.mapConserve((file) -> {
      try {
        System.out.println("IA" + PlatformDependent.usedDirectMemory());
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(file.path()), configuration);
        System.out.println("IB" + PlatformDependent.usedDirectMemory());
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        System.out.println("IC" + PlatformDependent.usedDirectMemory());
        int ret = Trivedi(reader, offset[0]);
        System.out.println("ID" + PlatformDependent.usedDirectMemory());
        offset[0] += ret;
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).toStream().reduceOption(Integer::sum).getOrElse(() -> -1);

    System.out.println("J" + PlatformDependent.usedDirectMemory());
    setVectors();
    System.out.println("K" + PlatformDependent.usedDirectMemory());
    vectorSchemaRoot.setRowCount(totalRows);
    System.out.println("L" + PlatformDependent.usedDirectMemory());
    t7 = System.nanoTime();
    if (t6 != null)
      time.add((t7 - t6) / 1e9d);
  }

  /***
   * Main logic of the ParquetToArrowConverter.
   * It takes a string-formatted path from which to read the Parquet input file, then proceeds
   * to retrieve its schema and convert it to an Arrow schema using
   * [[org.apache.parquet.arrow.schema]] operations available (SchemaConverter and SchemaMapping).
   *
   * Once the Arrow schema is created, it's then used to create an [[org.apache.arrow.vector.VectorSchemaRoot]]
   * which is then populated using the values read from the Parquet file, where each Parquet column
   * then corresponds to an Arrow column (or [[org.apache.arrow.vector.FieldVector]]).
   *
   * This method has no return value, it just creates the VectorSchemaRoot and its associated
   * vectors and populates them.
   * There's therefore a series of helper methods to retrieve specific vectors from the VectorSchemaRoot.
   *
   * So far, only INT32 (Int), INT64 (BigInt or Long) and BINARY (VarBinary, String/Array[Byte])
   * have been implemented. The rest of the primitive data types can be easily implemented in future.
   */
  public void process(String path) throws Exception {
    t2 = System.nanoTime();
    HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(path), configuration);
    ParquetFileReader reader = ParquetFileReader.open(inputFile);
    parquetSchema = reader.getFileMetaData().getSchema();
    t3 = System.nanoTime();
    time.add((t3 - t2) / 1e9d);

    t4 = System.nanoTime();
    SchemaConverter converter = new SchemaConverter();
    SchemaMapping mapping = converter.fromParquet(parquetSchema);
    arrowSchema = mapping.getArrowSchema();
    vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
    rowsCount = new ArrayList<>(Collections.nCopies(vectorSchemaRoot.getFieldVectors().size(), 0));
    t5 = System.nanoTime();
    time.add((t5 - t4) / 1e9d);

    int total_rows = Trivedi(reader, 0);

    setVectors();
    vectorSchemaRoot.setRowCount(total_rows);
    t7 = System.nanoTime();
    time.add((t7 - t6) / 1e9d);
  }

  private int Trivedi(ParquetFileReader reader, int offset) throws Exception {
    /* 26.08: The following part has been taken from A. Trivedi's implementation directly,
     * see @link{https://gist.github.com/animeshtrivedi/76de64f9dab1453958e1d4f8eca1605f}
     *
     * In case a better alternative arises, it will be included here.
     * 03.05.2022: Function was adapted s.t. multiple parquet files can be supported */
    t6 = System.nanoTime();
    List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
    List<FieldVector> vectors = vectorSchemaRoot.getFieldVectors();
    int size = colDesc.size();
    PageReadStore pageReadStore = reader.readNextRowGroup();
    int total_rows = 0;
    System.out.println("Allocated: " + allocator.getAllocatedMemory());

    while (pageReadStore != null) {
      ColumnReadStoreImpl colReader =
              new ColumnReadStoreImpl(
                      pageReadStore,
                      new DumpGroupConverter(),
                      parquetSchema,
                      reader.getFileMetaData().getCreatedBy());

      int rows = (int) pageReadStore.getRowCount();
      total_rows = Math.max(total_rows, rows);
      int i = 0;
      while (i < size) {
        System.out.println("Allocated (2)(" + i + "): " + allocator.getAllocatedMemory());
        ColumnDescriptor col = colDesc.get(i);
        ColumnReader cr = colReader.getColumnReader(col);
        int dmax = col.getMaxDefinitionLevel();
        switch (col.getPrimitiveType().getPrimitiveTypeName()) {
          case INT32: {
            ValueVectorFiller<IntVector> filler = (vector, index) -> vector.setSafe(index, cr.getInteger());
            writeColumn(BaseFixedWidthVector::setNull, filler, IntVector.class, cr, dmax, vectors.get(i), total_rows, offset);
            break;
          }
          case INT64: {
            ValueVectorFiller<BigIntVector> filler = (vector, index) -> vector.setSafe(index, cr.getLong());
            writeColumn(BaseFixedWidthVector::setNull, filler, BigIntVector.class, cr, dmax, vectors.get(i), total_rows, offset);
            break;
          }
          case BINARY: {
            ValueVectorFiller<BaseVariableWidthVector> filler = (vector, index) -> {
              vector.setIndexDefined(index);
              byte[] data = cr.getBinary().getBytes();
              vector.setValueLengthSafe(index, data.length);
              vector.setSafe(index, data);
            };
            writeColumn(BaseVariableWidthVector::setNull, filler, BaseVariableWidthVector.class, cr, dmax, vectors.get(i), total_rows, offset);
            break;
          }
          default:
            throw new Exception("Unsupported primitive type");
        }
        rowsCount.set(i, rowsCount.get(i)+ total_rows);
        i++;
      }
      pageReadStore = reader.readNextRowGroup();
    }
    return total_rows;
  }

  public List<Double> getTime() {
    return time;
  }

  /******* Helper public methods to retrieve VectorSchemaRoot and individual vectors *******/
  public VectorSchemaRoot getVectorSchemaRoot() {
    return vectorSchemaRoot;
  }

  public Optional<IntVector> getIntVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.INT)) return Optional.of((IntVector) v);
    }
    return Optional.empty();
  }

  public Optional<BigIntVector> getBigIntVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.BIGINT)) return Optional.of((BigIntVector) v);
    }
    return Optional.empty();
  }

  public Optional<BaseVariableWidthVector> getVariableWidthVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      ArrowType.ArrowTypeID id = v.getField().getType().getTypeID();
      if (id.equals(ArrowType.ArrowTypeID.Binary)) return Optional.of((BaseVariableWidthVector) v);
    }
    return Optional.empty();
  }

  public Optional<VarCharVector> getVarCharVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.VARCHAR)) return Optional.of((VarCharVector) v);
    }
    return Optional.empty();
  }

  public Optional<VarBinaryVector> getVarBinaryVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.VARBINARY)) return Optional.of((VarBinaryVector) v);
    }
    return Optional.empty();
  }


  private interface ValueVectorFiller<T> {
    /**
     * Places a new value in the vector at given index.
     * Function should be safe in the sense that the vector is extended if index > vector.size()
     * @param vector the vector to place the value in
     * @param index index at which to place the value
     */
    void setSafe(T vector, int index);
  }

  private interface ValueVectorNullFiller<T> {
    /**
     * Sets the value of the vector at given index to null
     * Function should be safe in the sense that the vector is extended if index > vector.size()
     * @param vector the vector to place the value in
     * @param index index at which to place the value
     */
    void setNullSafe(T vector, int index);
  }


  private <T extends ValueVector> void writeColumn(ValueVectorNullFiller<T> nullFiller, ValueVectorFiller<T> filler, Class<T> clazz, ColumnReader cr, int dmax, FieldVector v, int rows, int offset) {
    T vector = clazz.cast(v);
    if (vector.getValueCapacity() < 1) {
      vector.setInitialCapacity(rows);
      vector.allocateNew();
    }
    for (int i = 0; i < rows; ++i) {
      if (cr.getCurrentDefinitionLevel() == dmax) filler.setSafe(vector, i+offset);
      else nullFiller.setNullSafe(vector, i+offset);
      cr.consume();
    }
//    vector.setValueCount(rows);
  }
}
