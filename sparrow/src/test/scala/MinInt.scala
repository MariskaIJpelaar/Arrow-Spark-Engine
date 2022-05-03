import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.spark.{ArrowSparkContext, SparkConf}
import org.scalatest.funsuite.AnyFunSuite

class MinInt extends AnyFunSuite {
  private val size = 100 * 1000 // 100 k
  private val numPart = 10

  test("ArrowRDD vectorMin()") {
    // initialize vector
    var vector = new IntVector("vector", new RootAllocator())
    0 until size foreach { i =>
      vector.setSafe(i, i)
    }
    vector.setValueCount(size)

    // create SparkContext
    val sparkConf = new SparkConf()
      .setAppName("MinimumValue")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","logs")
    sparkConf.setMaster("local[*]")
    val asContext = new ArrowSparkContext(sparkConf)
    asContext.setLogLevel("ERROR")

    // run min
    assert(asContext.makeArrowRDD[Int](Array[ValueVector](vector), numPart).vectorMin() == 0)
  }

  test("Simple call to getMin() for IntVector") {
    val vector = new IntVector("vector", new RootAllocator())
    0 until size foreach { i =>
      vector.setSafe(i, i)
    }
    vector.setValueCount(size)
    assert(vector.getMin == 0)
  }
}
