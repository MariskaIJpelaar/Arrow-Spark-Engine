package utils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.List;

// from: https://stackoverflow.com/a/39734610 (2022-03-18)
// https://www.programcreek.com/java-api-examples/?api=org.apache.avro.generic.GenericRecordBuilder (2022-03-18)

public class ParquetWriter {
    private static final Schema default_schema = SchemaBuilder.builder("simple").record("record")
            .fields().requiredInt("id").requiredBoolean("gender_male").requiredString("name")
            .endRecord();

    public static Schema get_default_schema() { return default_schema; }

    public static void write_default_simple(OutputFile fileToWrite) {
        List<GenericData.Record> recordsToWrite = List.of(
                new GenericRecordBuilder(default_schema).set("id", 1).set("gender_male", true).set("name", "John").build(),
                new GenericRecordBuilder(default_schema).set("id", 2).set("gender_male", false).set("name", "Suzie").build(),
                new GenericRecordBuilder(default_schema).set("id", 3).set("gender_male", true).set("name", "Peter").build()
        );
        write(fileToWrite, default_schema, recordsToWrite);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
