import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.{ArrowSparkContext, SparkConf}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import utils.ParquetWriter

import java.nio.file.{Files, Paths}
import java.util

// TODO: test does not work yet... Need to find a method to clear after generate s.t. I can generate more than I can read?
// current VM option: -Xmx39m
class NotEnoughMemoryTest extends AnyFunSuite with BeforeAndAfterAll {
  private val directory_name = "data/numbers"
  private val num_files = 10
  private val amount = (10 * 1000 * 1000) / num_files
  private val schema = SchemaBuilder.builder("simple_int").record("record").fields.requiredInt("num").endRecord

  def generate_file_name(i: Int): String = {
    "nums" + i + "_" + amount + ".parquet"
  }

  override def beforeAll(): Unit = {
    Files.createDirectories(Paths.get(directory_name))
    val data: java.util.List[GenericData.Record] = new java.util.ArrayList()
    0 until amount foreach { i =>
      data.add(new GenericRecordBuilder(schema).set("num", i).build())
    }
    val batch: java.util.List[ParquetWriter.Writable] = new util.ArrayList[ParquetWriter.Writable]()
    0 until num_files foreach { i =>
      batch.add(new ParquetWriter.Writable(HadoopOutputFile.fromPath(new Path(directory_name, generate_file_name(i)), new Configuration), data))
    }
    ParquetWriter.write_batch(schema, batch, false)
  }

  override def afterAll(): Unit = {
    0 until num_files foreach { i =>
      Files.deleteIfExists(Paths.get(directory_name, generate_file_name(i)))
    }
  }

  test("ArrowRDD vectorMin() with small amount of memory") {
    // create SparkContext
    val sparkConf = new SparkConf()
      .setAppName("MinimumValue")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
    sparkConf.setMaster("local[*]")
    val asContext = new ArrowSparkContext(sparkConf)
    asContext.setLogLevel("ERROR")

    // run min
//    assert(asContext.makeArrowRDD[Int](Array[ValueVector](vector), numPart).vectorMin() == 0)

//    val handler = new ParquetToArrowConverter
//    handler.prepareDirectory(Directory(File(directory_name)))
//    var intArr = Array[ValueVector]();
//    var intRDD = asContext.makeArrowRDD[Int](intArr, 10)
//    while (handler.processFromDirectory()) {
//      intArr = Array[ValueVector](handler.getIntVector.get())
//      intRDD.union(asContext.makeArrowRDD[Int](intArr, 10))
//    }
//    assert(intRDD.min() == 0)
//    assert(intRDD.count() == amount * num_files)

//    handler.process(Directory(File(directory_name)))
  }

}
