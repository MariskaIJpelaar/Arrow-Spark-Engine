package org.apache.spark.sql.execution.datasources

import org.apache.arrow.vector.ValueVector
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * ArrowFilePartition to deal with partitions from ArrowFiles
 * inspired from: org/apache/spark/sql/execution/datasources/FilePartition
 */

case class PartitionArrowDirectory(values: Array[ValueVector], files: Seq[FileStatus])

case class ArrowFilePartition(index: Int, files: Array[PartitionedArrowFile])
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (_, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, _) => host
    }.toArray
  }
}

object ArrowFilePartition extends Logging {
  def getFilePartitions(
       sparkSession: SparkSession,
       partitionedFiles: Seq[PartitionedArrowFile],
       maxSplitBytes: Long): Seq[ArrowFilePartition] = {
    val partitions = new ArrayBuffer[ArrowFilePartition]
    val currentFiles = new ArrayBuffer[PartitionedArrowFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = ArrowFilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions
  }

  def maxSplitBytes(
                     sparkSession: SparkSession,
                     selectedPartitions: Seq[PartitionArrowDirectory]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
