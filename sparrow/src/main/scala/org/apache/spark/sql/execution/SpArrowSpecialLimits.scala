package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, ReturnAnswer}

/** Plans special cases of limit operators
 *  Similar to: SpecialLimits in SparkStrategies.scala (org.apache.spark.sql.execution) */
case class SpArrowSpecialLimits(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReturnAnswer(rootPlan) => rootPlan match {
      case Limit(IntegerLiteral(limit), child) =>
        // TODO: create own planLater which inherits from ArrowLimit??
        ArrowCollectLimitExec(limit, planLater(child).asInstanceOf[ArrowLimit]) :: Nil
      case other => planLater(other) :: Nil
    }
    case _ => Nil
  }

}
