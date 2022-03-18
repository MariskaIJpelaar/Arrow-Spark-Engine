import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.parquet.ParquetToArrowConverter;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import utils.ParquetWriter;

import java.io.IOException;

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
        ParquetWriter.write_default_simple(HadoopOutputFile.fromPath(new Path(filename), new Configuration()));
    }


    @Test
    @DisplayName("First test")
    void test() throws IOException {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(filename), new Configuration());
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        Schema other = ParquetWriter.get_default_schema();
//        Schema own = reader.getFileMetaData().getSchema();

        //TODO: compare them with assertEquals

    }
}
