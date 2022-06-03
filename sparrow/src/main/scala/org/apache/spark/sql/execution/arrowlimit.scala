package org.apache.spark.sql.execution

import org.apache.spark.rdd.{ArrowPartition, ParallelCollectionRDD, RDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}

trait ArrowLimit extends SparkPlan {
  def executeTakeArrow(n: Int): Array[ArrowPartition]
}

case class ArrowCollectLimitExec(limit: Int, child: ArrowLimit) extends LimitExec {
  private val exec = CollectLimitExec(limit, child)
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  override def executeCollect(): Array[InternalRow] = child.executeTakeArrow(limit).asInstanceOf[Array[InternalRow]]
  def executeArrowCollect(): Array[ArrowPartition] = child.executeTakeArrow(limit)

  /** Note: copied from limit.scala:CollectLimitExec */
  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    if (childRDD.getNumPartitions == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[InternalRow], 1, Map.empty)
    } else {
      val singlePartitionRDD = if (childRDD.getNumPartitions == 1) {
        childRDD
      } else {
        val locallyLimited = childRDD.mapPartitionsInternal(_.take(limit))
        new ShuffledRowRDD(
          ShuffleExchangeExec.prepareShuffleDependency(
            locallyLimited,
            child.output,
            SinglePartition,
            serializer,
            writeMetrics),
          readMetrics)
      }
      singlePartitionRDD.mapPartitionsInternal(_.take(limit))
    }
  }
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = exec.withNewChildrenInternal(IndexedSeq(newChild))
  override def output: Seq[Attribute] = exec.output
}
