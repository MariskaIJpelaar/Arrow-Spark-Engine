package org.apache.spark.sql.execution

import org.apache.spark.rdd.{ArrowPartition, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

trait ArrowLimit extends SparkPlan {
  def executeTakeArrow(n: Int): Array[ArrowPartition]
}


case class ArrowCollectLimitExec(limit: Int, child: ArrowLimit) extends LimitExec {
  private val exec = CollectLimitExec(limit, child)

  def executeArrowCollect(): Array[ArrowPartition] = child.executeTakeArrow(limit)

  override protected def doExecute(): RDD[InternalRow] = exec.execute()
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = exec.withNewChildrenInternal(IndexedSeq(newChild))
  override def output: Seq[Attribute] = exec.output
}
