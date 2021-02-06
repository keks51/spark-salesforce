package com.keks.sf.util

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer


/**
  * Computing time of operation takes.
  */
class BatchTimeMetrics extends SystemClock {

  private val BATCH_RESPONSE_TIME = "BATCH_RESPONSE_TIME"
  private val BATCH_PROCESSING_TIME = "BATCH_PROCESSING_TIME"

  def withBatchResponseTimeTaken[T](body: => T): T = timeTaken(BATCH_RESPONSE_TIME)(body)
  def batchProcessingTimeTaken[T](body: => T): T = timeTaken(BATCH_PROCESSING_TIME)(body)

  def getBatchResponseTimeAvg: Long = getAvg(BATCH_RESPONSE_TIME)
  def getBatchProcessingTimeAvg: Long = getAvg(BATCH_PROCESSING_TIME)

}

abstract class SystemClock {

  private val currentDurationsMs = new TrieMap[String, ArrayBuffer[Long]]()

  private def getTimeMillis: Long = System.currentTimeMillis()

  protected def timeTaken[T](metricName: String)(body: => T): T = {
    val startTime: Long = getTimeMillis
    val result: T = body
    val endTime: Long = getTimeMillis
    val timeTaken: Long = math.max(endTime - startTime, 0)
    currentDurationsMs.get(metricName).map(e => e += timeTaken)
      .getOrElse(currentDurationsMs += metricName -> ArrayBuffer(timeTaken))
    result
  }

  protected def getAvg(metricName: String): Long = {
    currentDurationsMs
      .get(metricName)
      .map(times => times.sum / times.length)
      .getOrElse(0L)
  }

}
