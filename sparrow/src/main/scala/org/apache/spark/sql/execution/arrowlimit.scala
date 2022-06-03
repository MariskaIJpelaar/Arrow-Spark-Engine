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

case class ArrowCollectLimitExec(limit: Int, child: SparkPlan) extends LimitExec {
  private val exec = CollectLimitExec(limit, child)

  override def executeCollect(): Array[InternalRow] = child.asInstanceOf[ArrowLimit].executeTakeArrow(limit).asInstanceOf[Array[InternalRow]]
  def executeArrowCollect(): Array[ArrowPartition] = child.asInstanceOf[ArrowLimit].executeTakeArrow(limit)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  /** Note: copied from limit.scala:CollectLimitExec */
  override protected def doExecute(): RDD[InternalRow] = exec.execute()
  override def output: Seq[Attribute] = exec.output
}
