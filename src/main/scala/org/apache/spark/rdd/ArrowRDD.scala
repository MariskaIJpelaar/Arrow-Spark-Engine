package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.spark._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * An Arrow-backed RDD using ValueVector as data input.
 * The logic used is similar to the one in ParallelCollectionRDD, with the only difference being that most of the
 * logic to extract the data from the vectors (and the resulting value's data type) is delegated to the partitions
 * themselves.
 *
 * Notice that it allows a vector of ValueVector(s) as input, only to determine in the compute(...) method if it's
 * a single vector or more.
 * Only the 1-D and 2-D array cases have been implemented for sake of clarity, in order to show the feasibility
 * of the overall solution rather than an omni-comprehensive work (which would've required too much time and resources).
 *
 * Plus, 3-D vectors or higher would require implementing the corresponding ArrowComposite_N_Partition which would
 * allow TupleN (N >= 3), thus increasing the effort and not providing any more meaningful result in terms of
 * overall feasibility
 *
 * @param sc the SparkContext associated with this RDD
 * @param data the Array of ValueVector(s) used to create this RDD
 * @param numSlices the level of parallelism / no. of partitions of this RDD (default = 1)
 * @param locationPrefs location preferences for the computation. Not defined
 * @tparam T the RDD primitive data type (as defined by the Scala Standard)
 */
private [spark] class ArrowRDD[T: ClassTag](@transient sc: SparkContext,
                                                    @transient val data: Array[ValueVector],
                                                    val numSlices: Int,
                                                    val locationPrefs: Map[Int, Seq[String]])
                                                    (implicit tag: TypeTag[T])
                                                    extends RDD[T](sc, Nil) with Logging {

  /* As said earlier, more vectors haven't been implemented as input for this RDD */
  private val _len = data.length
  require(_len <= 2, "Required maximum two ValueVector as parameter")


  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    _len match {
      case 1 => new InterruptibleIterator(context, split.asInstanceOf[ArrowPartition].iterator.asInstanceOf[Iterator[T]])
      case 2 => new InterruptibleIterator(context, split.asInstanceOf[ArrowCompositePartition].iterator.asInstanceOf[Iterator[T]])
      case _ => throw new SparkException("Required maximum two ValueVector as parameter")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    _len match {
      case 1 =>
        val slices = ArrowRDD.slice[T](data.head, numSlices) //only one element is in there, so it's also the first
        slices.indices.map(i => new ArrowPartition(id, i, slices(i))).toArray
      case 2 =>
        val tupleSliced = ArrowRDD.sliceAndTuplify[T](data, numSlices)
        tupleSliced.indices.map(i => new ArrowCompositePartition(id, i, tupleSliced(i))).toArray
      case _ => throw new SparkException("Required maximum two ValueVector as parameter")
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }

  /***
   * Returns a new ArrowRDD by applying a function to each element of this ArrowRDD.
   * It doesn't actually override RDD.map(f: T => U) because of the extra complexity added by using TypeTags!
   *
   */
  def map[U: ClassTag](f: T => U)(implicit tag: TypeTag[U], tag2 : TypeTag[T]) : ArrowRDD[U] = {
    /* If T and U are the same type (i.e. long => long), then there's no need to change the underlying vector's structure
    * Otherwise, some changes may be required to change from type to type (i.e. long => String) */
    if (tag.tpe == tag2.tpe) defaultMap(f)

    /* Return type (classTag[U]), from which the whole logic for creating a new ArrowRDD depends on */
    val cl = tag.tpe.baseClasses.head

    /* The following logic first checks the return type, then based on the original type it calls the correct
    * function createRddFromTransformation. Note that the case of T == U has been handled before.
    * The checks are performed using string representations (to avoid confusion with reflection types and naming */

    //TODO: change checks to not use string representation
    if (cl.toString.contains("String")){
      /* Starting RDD can't be ArrowRDD[String] (use defaultMap() instead) */
      createRddFromTransformation[U, T](f, data.head, numSlices, 1, "String")
    }
    else if (cl.toString.contains("Long")){
      /* Starting RDD can't be ArrowRDD[Long] (use defaultMap() instead) */
      createRddFromTransformation[U, T](f, data.head, numSlices, 1, "Long")
    }
    else if (cl.toString.contains("Int")){
      createRddFromTransformation[U, T](f, data.head, numSlices, 1, "Int")
    }
    else if (cl.toString.contains("Tuple2")){
      /* There's two typeArgs that define the Tuple2 type for each parameter. In case of a normal transformation, the second
      * typeArg contains the result type (i.e. Long => (Long, String), where the function only applies to the second element of
      * the tuple -for simplicity, that's the easiest case) */
      val tpe = tag.tpe.typeArgs.last

      if (tpe.toString.contains("Long")){
        createRddFromTransformation[U, T](f, data.head, numSlices, 2, "Long")
      }
      else if (tpe.toString.contains("Int")){
        createRddFromTransformation[U, T](f, data.head, numSlices, 2, "Int")
      }
      else if (tpe.toString.contains("String")){
        createRddFromTransformation[U, T](f, data.head, numSlices, 2, "String")
      }
      else throw new SparkException("Unsupported transformation in Tuple2")
    }
    else throw new SparkException("Unsupported return type [U] in transformation")
  }

  /***
   * The default map(f : T => U) implementation, as directly taken from RDD.scala
   * It's used only when the function f only applies a transformation on the values, not their types.
   * In that case, the whole Arrow-native vector support needs to change to accomodate the newly created vector(s).
   *
   * @return a MapPartitionsArrowRDD, where T == U
   */
  def defaultMap[U: ClassTag](f: T => U)(implicit tag: TypeTag[U], tag2: TypeTag[T]): ArrowRDD[U] = {
    val cleanF = sc.clean(f)
    new MapPartitionsArrowRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }

  override def filter(f: T => Boolean): ArrowRDD[T] = {
    val cleanF = sc.clean(f)

    val newRDD = new MapPartitionsArrowRDD[T, T](this, (_, _, iter) => iter.filter(cleanF))
    newRDD.setPreservePartitioning()
    newRDD
  }

  /***
   * As the name suggests, this function is used to create a new Arrow-backed RDD when a function f has been applied to an
   * existing ArrowRDD, such as the case of narrow or wide transformations.
   * It creates an Arrow-backed RDD where each element of the previous one has been converted to the correct values after
   * the function f has been applied, thus creating the right ValueVector type according to the return type of the function.
   * In case of composite return types (Tuple2) it creates both vectors to be used, according to the type and the transformation itself.
   *
   * It works similarly to MapPartitionsRDD, but in this case the Arrow-native data structures are maintained
   * throughout the execution.
   *
   * @param f the function to be applied to each element of the initial ValueVector
   * @param vec the vector with which the ArrowRDD has been created
   * @param numSlices the no. of partitions for the new ArrowRDD
   * @param numVecs the no. of vectors required for the new RDD (2 in case of Tuple return type)
   * @param tpe the type of vector that needs to be created
   * @return a new ArrowRDD where the function f has been already applied to all elements, maintaining the Arrow structure
   */
  def createRddFromTransformation[U: ClassTag, T: ClassTag](f: T => U, @transient vec: ValueVector,
                                                            numSlices: Int,
                                                            numVecs: Int,
                                                            tpe: String)
                                                           (implicit tag : TypeTag[U], tag2 : TypeTag[T]) : ArrowRDD[U] = {
    var vecRes: ValueVector = new ZeroVector
    val valCount = vec.getValueCount
    val cleanF = sc.clean(f)

    tpe match {
      case "String" => {
        vecRes = new StringVector("vector", new RootAllocator(Long.MaxValue))
        vecRes.asInstanceOf[StringVector].allocateNew(valCount)
        for (i <- 0 until valCount){
          /* if numVecs is larger than one, it means the return type is of type Tuple2 so the function
          * applied also returns a tuple (thus the different logic in the "else" clause) */
          if (numVecs == 1){
            vecRes.asInstanceOf[StringVector]
                .setSafe(i, cleanF.apply(vec.getObject(i).asInstanceOf[T]).asInstanceOf[String])
          }
          else vecRes.asInstanceOf[StringVector]
                  .setSafe(i, cleanF.apply(vec.getObject(i).asInstanceOf[T]).asInstanceOf[(T, U)]._2
                  .asInstanceOf[String])
        }
        vecRes.setValueCount(valCount)
      }
      case "Long" => {
        vecRes = new BigIntVector("vector", new RootAllocator(Long.MaxValue))
        vecRes.asInstanceOf[BigIntVector].allocateNew(valCount)
        for (i <- 0 until valCount){
          if (numVecs == 1) {
            vecRes.asInstanceOf[BigIntVector].setSafe(i, cleanF.apply(vec.getObject(i).asInstanceOf[T]).asInstanceOf[Long])
          }
          else vecRes.asInstanceOf[BigIntVector]
                  .setSafe(i, cleanF.apply(vec.getObject(i).asInstanceOf[T]).asInstanceOf[(T, U)]._2
                  .asInstanceOf[Long])
        }
        vecRes.setValueCount(valCount)
      }
      case "Int" => {
        vecRes = new IntVector("vector", new RootAllocator(Long.MaxValue))
        vecRes.asInstanceOf[IntVector].allocateNew(valCount)
        for (i <- 0 until valCount){
          if (numVecs == 1) {
            vecRes.asInstanceOf[IntVector].setSafe(i, cleanF.apply(vec.getObject(i).asInstanceOf[T]).asInstanceOf[Int])
          }
          else vecRes.asInstanceOf[IntVector]
            .setSafe(i, cleanF.apply(vec.getObject(i).asInstanceOf[T]).asInstanceOf[(T, U)]._2
              .asInstanceOf[Int])
        }
        vecRes.setValueCount(valCount)
      }
      case _ => throw new SparkException("Unsupported conversion!")
    }

    /* Depending on the numbers of ValueVector required (one for a type-to-type conversion, two in case of a Tuple-like
    * conversion), it creates the correct number of parameters to call the ArrowRDD constructor with */
    var vecArray : Array[ValueVector] = null
    numVecs match{
      case 1 => {
        vecArray = Array[ValueVector](vecRes)
      }
      case 2 => {
        vecArray = Array[ValueVector](vec, vecRes)
      }
      case _ => throw new SparkException("ArrowRDD only supports 2 vectors at most!")
    }
    new ArrowRDD[U](this.sc, vecArray, numSlices, Map[Int, Seq[String]]())
  }

}

private object ArrowRDD {

   implicit def rddToPairRDDFunctions[K, V](rdd: ArrowRDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  def slice[T: ClassTag](vec: ValueVector,
                         numSlices: Int): Seq[ValueVector] = {

    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt

        (start, end)
      }
    }

    /**
     * Slice original ValueVector with zero-copy, using the Slicing operations described
     * in the Java Arrow API documentation page.
     * Then convert the resulting vectors in a sequence
     */
    positions(vec.getValueCount, numSlices).map {
      case (start, end) => {
        val allocator = vec.getAllocator
        val tp = vec.getTransferPair(allocator)

        tp.splitAndTransfer(start, end-start)
        tp.getTo
      }
    }.toSeq
  }

  /* Same as slice() above, but used with two vectors as input. This method uses zero-copy split of the vectors
  * and creates a Tuple2 with the slices, which will then be used to create an ArrowCompositePartition */
  def sliceAndTuplify[T: ClassTag](vectors: Array[ValueVector],
                                   numSlices: Int) : Seq[(ValueVector, ValueVector)] = {
    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    require(vectors(0).getValueCount == vectors(1).getValueCount, "Vectors need to be the same size")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt

        (start, end)
      }
    }

    //Both value vectors have the same length, so only one is used
    positions(vectors.head.getValueCount, numSlices).map {
      case (start, end) => {
        // use of head and last to fetch the two different vectors
        val alloc1 = vectors.head.getAllocator
        val tp1 = vectors.head.getTransferPair(alloc1)
        val alloc2 = vectors.last.getAllocator
        val tp2 = vectors.last.getTransferPair(alloc2)

        tp1.splitAndTransfer(start, end-start)
        tp2.splitAndTransfer(start, end-start)

        (tp1.getTo, tp2.getTo)
      }
    }.toSeq
  }
}