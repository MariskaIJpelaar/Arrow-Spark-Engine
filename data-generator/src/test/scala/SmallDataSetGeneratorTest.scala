import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths
import scala.reflect.io.Directory

class SmallDataSetGeneratorTest extends AnyFunSuite with BeforeAndAfterAll {
  private val size = 10
  private val path = "data/small_generated/"
  private val baseName = "generated_100"
  private val tableName = "small"

  test("Generate Small DataSet") {
    nl.lu.mijpelaar.Main.main(Array(
      "--amount", s"$size",
      "--local",
      "--path", path+baseName
    ))

    assert(Directory(path).exists)

    val spark = SparkSession.builder.appName("simple checker")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // read in the generated data
    spark.read.format("parquet").option("mergeSchema", "true").option("dbtable", tableName)
      .load(Paths.get(path).resolve("*").toString)
      .createTempView(tableName)

    val intArr = spark.table(tableName).collect().map( x => x.getInt(0)).sortWith( _ < _ )
    val expected = Array.range(0, size)
    assert(expected.sameElements(intArr))
  }

  override def afterAll(): Unit = {
    Directory(path).deleteRecursively()
  }

}