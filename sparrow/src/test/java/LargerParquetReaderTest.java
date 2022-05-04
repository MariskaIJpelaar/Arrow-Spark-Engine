import org.apache.arrow.parquet.ParquetToArrowConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import utils.ParquetWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Test to read in a single (~40MB) parquet file")
public class LargerParquetReaderTest {
    private static final String directory_name = "data/numbers";
    private static final int amount = 10 * 1000 * 1000;
    private static final Schema schema = SchemaBuilder.builder("simple_int").record("record")
            .fields().requiredInt("num").endRecord();
    private static final String file_name = "nums_" + amount + ".parquet";

    @BeforeAll
    static void generateParquet() throws IOException {
        Files.createDirectories(Paths.get(directory_name));

        List<GenericData.Record> data = IntStream.range(0, amount).boxed()
                .map(i -> new GenericRecordBuilder(schema).set("num", i).build())
                .collect(Collectors.toList());

        OutputFile file = HadoopOutputFile.fromPath(new Path(directory_name, file_name), new Configuration());
        ParquetWriter.write(file, schema, data);
    }

    @AfterAll
    static void cleanParquet() throws IOException {
        Files.deleteIfExists(Paths.get(directory_name, file_name));
    }

    @Test
    @DisplayName("Test the generated parquet file")
    void larger_file() throws Exception {
        java.nio.file.Path path = Paths.get(directory_name, file_name);
        assertTrue(Files.exists(path));
        ParquetToArrowConverter handler = new ParquetToArrowConverter();
        handler.process(path.toString());
        assertTrue(handler.getVectorSchemaRoot().equals(ParquetWriter.get_vector_schema_root()));
    }

}
