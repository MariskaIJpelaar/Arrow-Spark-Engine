package org.apache.spark.sql.util

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.datasources.ArrowFileSourceStrategy

object SpArrowExtensionWrapper {
  type ExtensionBuilder = SparkSessionExtensions => Unit

  val injectArrowFileSourceStrategy: ExtensionBuilder = { e => e.injectPlannerStrategy(ArrowFileSourceStrategy) }
}


