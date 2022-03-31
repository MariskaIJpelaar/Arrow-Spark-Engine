import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.util.Text
import org.apache.arrow.vector.{ValueVector, VarCharVector}
import org.apache.spark.{ArrowSparkContext, SparkConf}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets

// TODO: perhaps change style later? Although we should not wait too long
// inspiration: https://www.scalatest.org/user_guide/selecting_a_style
class MakeArrowRDDTest extends AnyFunSuite {
  val default_num_part = 10

  test("simple makeArrowRDD from VarCharVector") {
    // generate data
    val names: Array[String] = Array("John", "Jeanette", "James")
    val vector: VarCharVector = new VarCharVector("a", new RootAllocator())
    vector.allocateNew(names.length)
    names.zipWithIndex.foreach { case(name, index) =>
      vector.set(index, name.getBytes(StandardCharsets.UTF_8))
    }
    vector.setValueCount(names.length)

    // setup ArrowSparkContext
    val sparkConf = new SparkConf()
      .setAppName("WordCount")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "3048576")
    sparkConf.setMaster("local[*]")
    val sc: ArrowSparkContext = new ArrowSparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    // performtest
    val binArr = Array[ValueVector](vector)
    val binRDD = sc.makeArrowRDD[Array[Text]](binArr, default_num_part)
    binRDD.first()

    // not done with creating the test
    assert(false)
  }

}