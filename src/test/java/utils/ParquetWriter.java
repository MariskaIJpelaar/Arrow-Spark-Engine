package utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// from: https://stackoverflow.com/a/39734610 (2022-03-18)
// https://www.programcreek.com/java-api-examples/?api=org.apache.avro.generic.GenericRecordBuilder (2022-03-18)

public class ParquetWriter {
    private static final Schema default_schema = SchemaBuilder.builder("simple").record("record")
            .fields().requiredInt("id").requiredString("name")
            .endRecord();
    public static Schema get_default_schema() { return default_schema; }

    private static MessageType message_type = null;
    public static MessageType get_message_type() { return message_type; }

    public static void write_default_simple(OutputFile fileToWrite) {
        List<GenericData.Record> recordsToWrite = Arrays.asList(
                new GenericRecordBuilder(default_schema).set("id", 1).set("name", "John").build(),
                new GenericRecordBuilder(default_schema).set("id", 2).set("name", "Suzie").build(),
                new GenericRecordBuilder(default_schema).set("id", 3).set("name", "Peter").build()
        );
        write(fileToWrite, default_schema, recordsToWrite);
    }

    private static VectorSchemaRoot root = null;
    public static VectorSchemaRoot get_vector_schema_root() { return root; }

    private static void set_vector_schema_root(List<GenericData.Record> recordsToWrite) {
        BufferAllocator allocater = new RootAllocator(Integer.MAX_VALUE);
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new SchemaConverter().fromParquet(get_message_type()).getArrowSchema();

        List<FieldVector> field_vectors = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            FieldVector vector = field.createVector(allocater);

            switch (field.getType().getTypeID()) {
                case Int: ((IntVector) vector).allocateNew(recordsToWrite.size()); break;
                case Utf8: ((VarCharVector) vector).allocateNew(recordsToWrite.size()); break;
                default: throw new RuntimeException("[ParquetWriter] type not supported");
            }

            int i = 0;
            for (GenericData.Record record : recordsToWrite) {
                switch (field.getType().getTypeID()) {
                    case Int:
                        assert vector instanceof IntVector;
                        // we allocated enough space for all records, so we prefer set() over setSafe()
                        ((IntVector) vector).set(i, (int)record.get(field.getName()));
                        break;
                    case Utf8:
                        assert vector instanceof VarCharVector;
                        // we allocated enough space for all records, so we prefer set() over setSafe()
                        ((VarCharVector) vector).set(i, ((String)record.get(field.getName())).getBytes(StandardCharsets.UTF_8));
                        break;
                    default: throw new RuntimeException("[ParquetWriter] type not supported");
                }
                ++i;
            }
            vector.setValueCount(recordsToWrite.size());
            field_vectors.add(vector);
        }
        root = new VectorSchemaRoot(arrowSchema, field_vectors, recordsToWrite.size());
    }

    public static class Writable {
        public OutputFile fileToWrite;
        public List<GenericData.Record> recordsToWrite;

        public Writable(OutputFile fileToWrite, List<GenericData.Record> recordsToWrite) {
            this.fileToWrite = fileToWrite;
            this.recordsToWrite = recordsToWrite;
        }
    }

    public static void write_batch(Schema schema, List<Writable> writables) {
        List<GenericData.Record> total = new ArrayList<>(Collections.emptyList());
        writables.forEach( (writable) -> {
            try (org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                    .<GenericData.Record>builder(writable.fileToWrite)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
                for (GenericData.Record record : writable.recordsToWrite) {
                    writer.write(record);
                }
                writer.close(); // so we can get the Footer
                if (message_type == null)
                    message_type = writer.getFooter().getFileMetaData().getSchema();
                else
                    message_type.union(writer.getFooter().getFileMetaData().getSchema());
                total.addAll(writable.recordsToWrite);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        set_vector_schema_root(total);
    }

    public static void write(OutputFile fileToWrite, Schema schema, List<GenericData.Record> recordsToWrite) {
        try (org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
            writer.close(); // so we can get the Footer
            message_type = writer.getFooter().getFileMetaData().getSchema();
        } catch (IOException e) {
            e.printStackTrace();
        }
        set_vector_schema_root(recordsToWrite);
    }


}
