package com.keks.sf.soap

import org.apache.spark.Partition


case class SfSparkPartition(id: Int,
                            column: String,
                            lowerBound: String,
                            upperBound: String,
                            leftCondOperator: String,
                            rightCondOperator: String,
                            offsetValueIsString: Boolean,
                            var executorMetrics: ExecutorMetrics) extends Partition {

  override def index: Int = id

  override def toString = {
    s"IDX: '$id'. Column: '$column'. Clause: '$getWhereClause'"
  }

  def getWhereClause: String =
    s"$column $leftCondOperator $lowerBound AND $column $rightCondOperator $upperBound"

  private def update(f: ExecutorMetrics => ExecutorMetrics): Unit = {
    executorMetrics = f(executorMetrics)
  }

  def setOffset(value: String): Unit = update(_.copy(offset = value))

  def getOffset: String = {
    executorMetrics.offset
  }

  def setIsDone(): Unit = update(_.copy(isDone = true))

  def setQueryLocator(value: String): Unit = update(_.copy(queryLocator = Some(value)))

  def getQueryLocator: Option[String] = {
    executorMetrics.queryLocator
  }

  def setRecordsToLoad(recordsToLoad: Int): Unit = update(_.copy(recordsToLoad = recordsToLoad))

  def updateMetrics(loadedRecords: Int,
                    batchResponseTimeAvg: Long,
                    batchProcessingTimeAvg: Long,
                    partitionProcessingTime: Long,
                    recordsInSfBatch: Int): Unit =
    update(e => e.copy(loadedRecordsSum = loadedRecords + e.loadedRecordsSum,
                       loadedRecords = loadedRecords,
                       batchResponseTimeAvg = batchResponseTimeAvg,
                       batchSfProcessingTimeAvg = batchProcessingTimeAvg,
                       partitionProcessingTime = partitionProcessingTime,
                       recordsInSfBatch = recordsInSfBatch))

  def setLastProcessedRecordTime(time: Long): Unit = update(_.copy(lastProcessedRecordTime = time))

}

case class SfStreamingPartitions(partitions: Array[SfSparkPartition],
                                 maxUpperBoundOffset: Option[String])

