package nl.tudelft.ffiorini;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Class implementing reading from a Parquet file, converting the Parquet schema to
 * an Arrow schema and then produce an Arrow file as output.
 * It also creates a VectorSchemaRoot that can be used when integrating Arrow with Spark
 * (org.apache.arrow.vector Java API and Spark Scala API).
 *
 * Inspired by Animesh Trivedi implementation of ParquetToArrow.java
 *
 * Created on:      03-03-2021
 * Author:          ffiorini
 */
public class ParquetToArrowConverter
{
    private MessageType parquetSchema;
    private ParquetFileReader parquetReader;


    private RootAllocator ra = null;
    private Schema arrowSchema;
    private VectorSchemaRoot arrowRoot;
    private ArrowFileWriter arrowWriter;

    /**
     * Constructor
     */
    public ParquetToArrowConverter()
    {
        this.ra = new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * @return the VectorSchemaRoot created from the Parquet file
     */
    public VectorSchemaRoot getArrowRoot()
    {
        return arrowRoot;
    }
}
