package com.keks.sf.util


class BatchTimeMetrics extends SystemClock {

  private val BATCH_RESPONSE_TIME = "BATCH_RESPONSE_TIME"
  private val BATCH_PROCESSING_TIME = "BATCH_PROCESSING_TIME"

  def withBatchResponseTimeTaken[T](body: => T): T = timeTaken(BATCH_RESPONSE_TIME)(body)
  def batchProcessingTimeTaken[T](body: => T): T = timeTaken(BATCH_PROCESSING_TIME)(body)

  def getBatchResponseTimeAvg: Long = getAvg(BATCH_RESPONSE_TIME)
  def getBatchProcessingTimeAvg: Long = getAvg(BATCH_PROCESSING_TIME)

}
