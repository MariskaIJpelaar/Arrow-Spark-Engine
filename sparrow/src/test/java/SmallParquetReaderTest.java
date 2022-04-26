import org.apache.arrow.parquet.ParquetToArrowConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import utils.ParquetWriter;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tutorials which got me started: (2022-03-18)
 * https://www.petrikainulainen.net/programming/testing/junit-5-tutorial-running-unit-tests-with-maven/
 * https://www.petrikainulainen.net/programming/testing/junit-5-tutorial-writing-our-first-test-class/
 * https://junit.org/junit5/docs/current/user-guide/
 */

@DisplayName("Test to read a small parquet file with spArrow")
public class SmallParquetReaderTest {
    private static final String filename = "data/simple.parquet";

    @BeforeAll
    static void generateParquet() throws IOException {
        File file = new File(filename);
        assert !file.exists() || file.delete();
        ParquetWriter.write_default_simple(HadoopOutputFile.fromPath(new Path(filename), new Configuration()));
    }


    @Test
    @DisplayName("Test a simple file")
    void simple_test() throws Exception {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(filename), new Configuration());
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        MessageType other = ParquetWriter.get_message_type();
        assertNotNull(other);
        MessageType own = reader.getFileMetaData().getSchema();
        assertEquals(0, own.toString().compareTo(other.toString()));

        ParquetToArrowConverter handler = new ParquetToArrowConverter();
        handler.process(filename);
        assertTrue(handler.getVectorSchemaRoot().equals(ParquetWriter.get_vector_schema_root()));
    }
}
