package org.apache.spark.sql.execution

import org.apache.arrow.vector.ValueVector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, PlanExpression, Predicate}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable
import scala.reflect.ClassTag


trait ArrowFileFormat extends FileFormat {
  /** Returns a function that can be used to read a single file in as an Iterator of Array[ValueVector] */
  def buildArrowReaderWithPartitionValues(sparkSession: SparkSession,
                                     dataSchema: StructType,
                                     partitionSchema: StructType,
                                     requiredSchema: StructType,
                                     filters: Seq[Filter],
                                     options: Map[String, String],
                                     hadoopConf: Configuration) : PartitionedArrowFile => Iterator[Array[ValueVector]]
}

class ArrowScanExec(@transient relation: HadoopFsRelation,
                      output: Seq[Attribute],
                      requiredSchema: StructType,
                      partitionFilters: Seq[Expression],
                      optionalBucketSet: Option[BitSet],
                      optionalNumCoalescedBuckets: Option[Int],
                      dataFilters: Seq[Expression],
                      tableIdentifier: Option[TableIdentifier],
                      disableBucketedScan: Boolean = false) extends FileSourceScanExec(relation,
  output,
  requiredSchema,
  partitionFilters,
  optionalBucketSet,
  optionalNumCoalescedBuckets,
  dataFilters,
  tableIdentifier,
  disableBucketedScan) {

  // copied from org/apache/spark/sql/execution/DataSourceScanExec.scala
  @transient
  private lazy val pushedDownFilters = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  // copied and edited from org/apache/spark/sql/execution/DataSourceScanExec.scala
  private def createFileScanArrowRDD[T: ClassTag](
      readFunc: PartitionedArrowFile => Iterator[Array[ValueVector]],
      numBuckets: Int,
      selectedPartitions: Array[PartitionArrowDirectory]) : FileScanArrowRDD[T]  = {
    logInfo(s"Planning with ${numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          ArrowPartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(throw new IllegalStateException(s"Invalid bucket file ${f.filePath}"))
      }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val partitionedFiles = coalescedBuckets.get(bucketId).map {
          _.values.flatten.toArray
        }.getOrElse(Array.empty)
        ArrowFilePartition(bucketId, partitionedFiles)
      }
    }.getOrElse {
      Seq.tabulate(numBuckets) { bucketId =>
        ArrowFilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }
    }

    new FileScanArrowRDD(relation.sparkSession, readFunc, filePartitions)
  }

  // copied from org/apache/spark/sql/execution/DataSourceScanExec.scala
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  // copied from org/apache/spark/sql/execution/DataSourceScanExec.scala
  private lazy val driverMetrics: mutable.HashMap[String, Long] = mutable.HashMap.empty

  // copied from org/apache/spark/sql/execution/DataSourceScanExec.scala
  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
                                        partitions: Seq[PartitionDirectory],
                                        static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
  }

  // copied and edited from org/apache/spark/sql/execution/DataSourceScanExec.scala
  @transient lazy val selectedArrowPartitions: Array[PartitionArrowDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret.asInstanceOf[Seq[PartitionArrowDirectory]]
  }.toArray

  // copied and edited from org/apache/spark/sql/execution/DataSourceScanExec.scala
  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient private lazy val dynamicallySelectedPartitions: Array[PartitionArrowDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      // TODO: Change predicate?
      val ret = selectedArrowPartitions.filter(p => boundPredicate.eval(p.values.asInstanceOf[InternalRow]))
      setFilesNumAndSizeMetric(ret.toSeq.asInstanceOf[Seq[PartitionDirectory]], false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedArrowPartitions
    }
  }

  override lazy val inputRDD: RDD[InternalRow] = {
    val root: (PartitionedArrowFile) => Iterator[Array[ValueVector]] = relation.fileFormat.asInstanceOf[ArrowFileFormat].buildArrowReaderWithPartitionValues(
      relation.sparkSession, relation.dataSchema, relation.partitionSchema, requiredSchema, pushedDownFilters,
      relation.options,  relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
    )
    createFileScanArrowRDD(root, relation.bucketSpec.get.numBuckets, dynamicallySelectedPartitions)
  }
}
