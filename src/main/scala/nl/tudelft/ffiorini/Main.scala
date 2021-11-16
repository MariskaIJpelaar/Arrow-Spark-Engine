package nl.tudelft.ffiorini

import nl.tudelft.ffiorini.experiments.EvaluationSuite
import org.apache.spark.{ArrowSparkContext, SparkConf}

object Main {
  def main(args: Array[String]): Unit = {
      System.setProperty("hadoop.home.dir","C:/hadoop")
      val conf = new SparkConf()
        .setAppName("Example Program")
        .setMaster("local")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "3048576")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir","logs")
      val sc = new ArrowSparkContext(conf)
      
      /* Run experiments here */
      EvaluationSuite.wordCount(sc)
    
      EvaluationSuite.scalaSort(sc)
    
      EvaluationSuite.transformations(sc)
    
      EvaluationSuite.minimumValue(sc)
  }
}
