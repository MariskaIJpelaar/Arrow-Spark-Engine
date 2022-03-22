package utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
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

    private static List<FieldVector> fieldVectors = null;

    public static void write_default_simple(OutputFile fileToWrite) {
        List<GenericData.Record> recordsToWrite = List.of(
                new GenericRecordBuilder(default_schema).set("id", 1).set("name", "John").build(),
                new GenericRecordBuilder(default_schema).set("id", 2).set("name", "Suzie").build(),
                new GenericRecordBuilder(default_schema).set("id", 3).set("name", "Peter").build()
        );
        write(fileToWrite, default_schema, recordsToWrite);
    }

    private static RuntimeException setFieldVectors(Schema schema, List<GenericData.Record> recordsToWrite) {
        // TODO: implement!! Might be nonsense now...
        BufferAllocator allocater = new RootAllocator(Integer.MAX_VALUE);
        SchemaConverter converter = new SchemaConverter();
        SchemaMapping mapping = converter.fromParquet(get_message_type());
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = mapping.getArrowSchema();

        fieldVectors = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
//            Schema.Type type = field.schema().getType();
            FieldVector vector = field.createVector(allocater);

            switch (field.getType().getTypeID()) {
                case Int: ((IntVector) vector).allocateNew(); break;
                case Binary: ((VarCharVector) vector).allocateNew(); break;
                default: return new RuntimeException("[ParquetWriter] type not supported");
            }

            int i = 0;
            for (GenericData.Record record : recordsToWrite) {
                switch (field.getType().getTypeID()) {
                    case Int: ((IntVector) vector).setSafe();
                        break;
                    case Binary:
                        break;
                    default: return new RuntimeException("[ParquetWriter] type not supported");
                }
                ++i;
            }
            fieldVectors.add(vector);
        }
        return null;
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
        setFieldVectors(schema, recordsToWrite);
    }


}
