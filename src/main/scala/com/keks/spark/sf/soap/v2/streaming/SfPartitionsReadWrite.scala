package com.keks.spark.sf.soap.v2.streaming

import com.keks.spark.sf.LogSupport
import com.keks.spark.sf.soap.v2.streaming.SfPartitionsReadWrite.{SF_DRIVER_PARTITIONS, SF_EXECUTORS_PARTITIONS_DIR, SF_EXECUTOR_PARTITION_FILE_NAME}
import com.keks.spark.sf.soap.{SfSparkPartition, SfStreamingPartitions}
import com.keks.spark.sf.util.UniqueQueryId
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.ConcurrentModificationException


// TODO check hdfs conf with emr
/**
  * SfPartitions manager.
  * This class helps to read and keep information about data processed on executors.
  * Driver can read information about all executors.
  * Executor can save in information, that will be read by driver.
  * Mostly this class reuses functionality
  * from org.apache.spark.sql.execution.streaming.CheckpointFileManager.
  *
  * @param checkpointLocation streaming checkpoint dir location
  * @param conf hadoop conf
  */
case class SfPartitionsReadWrite(checkpointLocation: String,
                                 conf: Configuration)
                                (implicit uniqueQueryId: UniqueQueryId) extends LogSupport {

  implicit val formats: DefaultFormats.type = DefaultFormats

  private val fileManager = CheckpointFileManager.create(new Path(checkpointLocation), conf)

  /**
    * Reading metadata about all executors when microBatch finishes.
    */
  def readDriverSfPartitions: Option[SfStreamingPartitions] = {
    val sfPartitionsPath = new Path(s"$checkpointLocation/$SF_DRIVER_PARTITIONS")
    if (fileManager.exists(sfPartitionsPath)) {
      val input = fileManager.open(sfPartitionsPath)
      try {
        val reader = new InputStreamReader(input, StandardCharsets.UTF_8)
        val res = Some(Serialization.read[SfStreamingPartitions](reader))
        infoQ(s"Loaded driver partitions from path: '${sfPartitionsPath.toString}'")
        res
      } catch {
        case ise: IllegalStateException =>
          // re-throw the exception with the log file path added
          throw new IllegalStateException(
            s"QueryId: $uniqueQueryId. Failed to read $SF_DRIVER_PARTITIONS file in $sfPartitionsPath. ${ise.getMessage}", ise)
      } finally {
        IOUtils.closeQuietly(input)
      }
    } else {
      warnQ(s"Unable to find $SF_DRIVER_PARTITIONS file in path $sfPartitionsPath")
      None
    }
  }

  /**
    * Storing partitions before sending them to executors for fault tolerance.
    *
    * @param sfPartitions partitions
    */
  def writeDriverSfPartitions(sfPartitions: SfStreamingPartitions): Unit = {
    val offsetPath = new Path(s"$checkpointLocation")
    val sfPartitionsPath = new Path(s"$checkpointLocation/$SF_DRIVER_PARTITIONS")
    if (!fileManager.exists(offsetPath)) fileManager.mkdirs(offsetPath)
    val out: CheckpointFileManager.CancellableFSDataOutputStream =
      fileManager.createAtomic(sfPartitionsPath, overwriteIfPossible = true)
    try {
      Serialization.write(sfPartitions, out)
      out.close()
      infoQ(s"SfPartitions were saved in path: '$sfPartitionsPath''")
    } catch {
      case e: FileAlreadyExistsException =>
        out.cancel()
        // If next batch file already exists, then another concurrently running query has
        // written it.
        throw new ConcurrentModificationException(
          s"Multiple streaming queries are concurrently using $offsetPath", e)
      case e: Throwable =>
        out.cancel()
        throw e
    }
  }

  /**
    * Executor saves information about processed data for driver.
    *
    * @param partitionId unique partition id.
    * @param partition partition
    */
  def writeExecutorSfPartition(partitionId: Int, partition: SfSparkPartition): Unit = {
    val offsetPath = new Path(s"$checkpointLocation/$SF_EXECUTORS_PARTITIONS_DIR/$partitionId/")
    val offsetFilePath = new Path(s"$offsetPath/$SF_EXECUTOR_PARTITION_FILE_NAME")
    if (!fileManager.exists(offsetPath)) {
      fileManager.mkdirs(offsetPath)
    }
    val out: CheckpointFileManager.CancellableFSDataOutputStream =
      fileManager.createAtomic(offsetFilePath, overwriteIfPossible = true)
    try {
      Serialization.write(partition, out)
      out.close()
    } catch {
      case e: FileAlreadyExistsException =>
        out.cancel()
        // If next batch file already exists, then another concurrently running query has
        // written it.
        throw new ConcurrentModificationException(
          s"Multiple streaming queries are concurrently using $offsetPath", e)
      case e: Throwable =>
        out.cancel()
        throw e
    }
  }

  /**
    * Driver read information from all executors to recreate partitions.
    */
  def readExecutorPartitions: Option[Array[SfSparkPartition]] = {
    val offsetPath = new Path(s"$checkpointLocation/$SF_EXECUTORS_PARTITIONS_DIR/")
    val res: Array[SfSparkPartition] = if (fileManager.exists(offsetPath)) {
      val partitions = fileManager.list(offsetPath)
      partitions.flatMap { partitionPath =>
        val filePath = new Path(s"${partitionPath.getPath}/$SF_EXECUTOR_PARTITION_FILE_NAME")
        if (fileManager.exists(filePath)) {
          val input = fileManager.open(filePath)
          try {
            val reader = new InputStreamReader(input, StandardCharsets.UTF_8)
            Some(Serialization.read[SfSparkPartition](reader))
          } catch {
            case ise: IllegalStateException =>
              // re-throw the exception with the log file path added
              throw new IllegalStateException(
                s"Failed to read last_offset file in $filePath. ${ise.getMessage}", ise)
          } finally {
            IOUtils.closeQuietly(input)
          }
        } else {
          None
        }

      }
    } else {
      Array.empty[SfSparkPartition]
    }
    if (res.nonEmpty) Some(res) else None
  }

  /**
    * Deleting executors partitions.
    */
  def deleteExecutorsPartitionsDir(): Unit = {
    val offsetPath = new Path(s"$checkpointLocation/$SF_EXECUTORS_PARTITIONS_DIR")
    fileManager.delete(offsetPath)
  }

}

object SfPartitionsReadWrite {

  val SF_EXECUTORS_PARTITIONS_DIR = "sf_executor_parts"
  val SF_EXECUTOR_PARTITION_FILE_NAME = "sf_partition"
  val SF_DRIVER_PARTITIONS = "sf_driver_partitions.json"

}
