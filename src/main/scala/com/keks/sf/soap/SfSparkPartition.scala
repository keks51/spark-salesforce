package com.keks.sf.soap

import org.apache.spark.Partition


/**
  * Spark partition which contains data about Salesforce loading parameters.
  * Sometime partition can be reused, that's why executorMetrics is 'var'.
  *
  * @param id unique partition ID
  * @param column offset col name
  * @param lowerBound lowerBound offset
  * @param upperBound upperBound offset
  * @param leftCondOperator >= or >
  * @param rightCondOperator <= or <
  * @param offsetValueIsString if true then value is wrapped by '.
  *                            For example alex will be 'alex'.
  * @param executorMetrics Metadata about loading process on executor
  */
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

  /* Build where clause. For example: 'age >= 10 AND age <= 20' */
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

  override def equals(other: Any) = {
    other match {
      case other: SfSparkPartition =>
        other.canEqual(this) &&
          this.id == other.id &&
          this.column == other.column &&
          this.lowerBound == other.lowerBound &&
          this.upperBound == other.upperBound &&
          this.leftCondOperator == other.leftCondOperator &&
          this.rightCondOperator == other.rightCondOperator &&
          this.offsetValueIsString == other.offsetValueIsString &&
          this.executorMetrics == other.executorMetrics &&
          this.index == other.index
      case _ => false
    }
  }

}

/**
  * Storing all partitions with max offset from all partitions.
  * For example on start partitions were:
  * 'age >= 10 AND age < 20'
  * 'age >= 20 AND age < 30'
  * 'age >= 30 AND age < 40'
  * then maxUpperBoundOffset is '40'
  */
case class SfStreamingPartitions(partitions: Array[SfSparkPartition],
                                 maxUpperBoundOffset: Option[String])

