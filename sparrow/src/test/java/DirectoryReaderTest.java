import org.apache.arrow.parquet.ParquetToArrowConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.reflect.io.Directory;
import utils.ParquetWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Test to read in a directory of parquet files")
public class DirectoryReaderTest {
    private static final String directory_name = "data/numbers";
    private static final Schema schema = SchemaBuilder.builder("simple_int").record("record")
            .fields().requiredInt("num").endRecord();

    @BeforeAll
    static void generateParquets() throws IOException {
        Directory directory = new Directory(new File(directory_name));
        assert !directory.exists() || directory.deleteRecursively();

        List<GenericData.Record> firstHalf = Arrays.asList(
            new GenericRecordBuilder(schema).set("num", 1).build(),
            new GenericRecordBuilder(schema).set("num", 2).build(),
            new GenericRecordBuilder(schema).set("num", 3).build()
        );
        OutputFile firstFile = HadoopOutputFile.fromPath(new Path(directory.path(), "first.parquet"), new Configuration());


        List<GenericData.Record> secondHalf = Arrays.asList(
                new GenericRecordBuilder(schema).set("num", 4).build(),
                new GenericRecordBuilder(schema).set("num", 5).build(),
                new GenericRecordBuilder(schema).set("num", 6).build()
        );
        OutputFile secondFile = HadoopOutputFile.fromPath(new Path(directory.path(), "second.parquet"), new Configuration());

        ParquetWriter.write_batch(schema, Arrays.asList(
                new ParquetWriter.Writable(firstFile, firstHalf),
                new ParquetWriter.Writable(secondFile, secondHalf)
        ), true);
    }

    @Test
    @DisplayName("Test a small directory")
    void small_dir() throws IOException {
        ParquetToArrowConverter handler = new ParquetToArrowConverter();
        handler.process(new Directory(new File(directory_name)));
        assertTrue(handler.getVectorSchemaRoot().equals(ParquetWriter.get_vector_schema_root()));
    }
}
