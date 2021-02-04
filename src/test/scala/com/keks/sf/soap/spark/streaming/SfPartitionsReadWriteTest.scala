package com.keks.sf.soap.spark.streaming

import com.keks.sf.soap
import com.keks.sf.soap.v2.streaming.SfPartitionsReadWrite
import com.keks.sf.soap.v2.streaming.SfPartitionsReadWrite._
import com.keks.sf.soap.{ExecutorMetrics, SfSparkPartition, SfStreamingPartitions}
import org.apache.hadoop.conf.Configuration
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import utils.{TestBase, TmpDirectory}


class SfPartitionsReadWriteTest extends TestBase with TmpDirectory {

  implicit val formats: DefaultFormats.type = DefaultFormats
  private val COMMITS_DIR = "commits"
  private val METADATA_FILE_NAME = "metadata"

  "OffsetReadWrite#readDriverSfPartitions" should "read offset" in withTempDir { dir =>
    val checkPointDirPath = s"$dir/$CHECKPOINT_DIR"
    createDir(checkPointDirPath)
    val exp = SfStreamingPartitions(
      Array(
        SfSparkPartition(1, "id", "a", "b", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("a")),
        soap.SfSparkPartition(2, "id", "c", "d", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("c")),
        soap.SfSparkPartition(3, "id", "e", "f", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("e"))),
      Some("")
      )

    createFile(s"$checkPointDirPath", SF_DRIVER_PARTITIONS, write(exp))
    val checkPointReadWrite = SfPartitionsReadWrite(checkPointDirPath, new Configuration())
    val result: SfStreamingPartitions = checkPointReadWrite.readDriverSfPartitions.get
    exp.partitions.zip(result.partitions).foreach { case (x, y) =>
      assert(x.executorMetrics == y.executorMetrics)
      assert(x.id == y.id)
    }
    assert(exp.maxUpperBoundOffset == result.maxUpperBoundOffset)
  }

  "CheckPointReadWrite#writeDriverSfPartitions" should "write" in withTempDir { dir =>
    val checkPointDirPath = s"$dir/$CHECKPOINT_DIR"
    createDir(checkPointDirPath)
    val exp = SfStreamingPartitions(
      Array(
        soap.SfSparkPartition(1, "id", "a", "b", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("a")),
        soap.SfSparkPartition(2, "id", "c", "d", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("c")),
        soap.SfSparkPartition(3, "id", "e", "f", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("e"))),
      Some("")
      )
    val checkPointReadWrite = SfPartitionsReadWrite(checkPointDirPath, new Configuration())
    checkPointReadWrite.writeDriverSfPartitions(exp)
    val expectedFilePath = s"$checkPointDirPath/$SF_DRIVER_PARTITIONS"
    assertFileExists(expectedFilePath)
    val expectedFileContent = write(exp)
    assertFileContentIs(expectedFilePath, expectedFileContent)
  }

  "CheckPointReadWrite#writeExecutorSfPartition" should "overwrite" in withTempDir { dir =>
    val checkPointDirPath = s"$dir/$CHECKPOINT_DIR"
    createDir(checkPointDirPath)
    createDir(s"$checkPointDirPath/$COMMITS_DIR")
    createDir(s"$checkPointDirPath/$SF_EXECUTORS_PARTITIONS_DIR")
    createFile(checkPointDirPath, METADATA_FILE_NAME, "")

    val partitionId = 0
    val checkPointReadWrite = SfPartitionsReadWrite(checkPointDirPath, new Configuration())
    val x1 = soap.SfSparkPartition(1, "id", "a", "b", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("a"))
    val x2 = soap.SfSparkPartition(1, "id", "b", "c", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("b"))

    checkPointReadWrite.writeExecutorSfPartition(partitionId, x1)
    checkPointReadWrite.writeExecutorSfPartition(partitionId, x2)

    val expectedFilePath = s"$checkPointDirPath/$SF_EXECUTORS_PARTITIONS_DIR/$partitionId/$SF_EXECUTOR_PARTITION_FILE_NAME"
    assertFileExists(expectedFilePath)
    val expectedFileContent = write(x2)
    assertFileContentIs(expectedFilePath, expectedFileContent)
  }

  "CheckPointReadWrite#readExecutorPartitions" should "read" in withTempDir { dir =>
    val checkPointDirPath = s"$dir/$CHECKPOINT_DIR"
    createDir(checkPointDirPath)
    createDir(s"$checkPointDirPath/$COMMITS_DIR")
    createDir(s"$checkPointDirPath/$SF_EXECUTORS_PARTITIONS_DIR")
    createFile(checkPointDirPath, METADATA_FILE_NAME, "")


    val checkPointReadWrite = SfPartitionsReadWrite(checkPointDirPath, new Configuration())
    val x1 = soap.SfSparkPartition(1, "id", "a", "b", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("a"))
    val x2 = soap.SfSparkPartition(2, "id", "b", "c", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("b"))
    val x3 = soap.SfSparkPartition(3, "id", "c", "c", ">=", ">=", offsetValueIsString = true, ExecutorMetrics("c"))

    checkPointReadWrite.writeExecutorSfPartition(1, x1)
    checkPointReadWrite.writeExecutorSfPartition(2, x2)
    checkPointReadWrite.writeExecutorSfPartition(3, x3)

    val res = checkPointReadWrite.readExecutorPartitions.get
    Array(x1, x2, x3).zip(res).foreach { case (x, y) =>
      assert(x.executorMetrics == y.executorMetrics)
      assert(x.id == y.id)
    }
  }

}
