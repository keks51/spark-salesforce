package com.keks.sf.util

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer


abstract class SystemClock {

  private val currentDurationsMs = new TrieMap[String, ArrayBuffer[Long]]()

  private def getTimeMillis: Long = System.currentTimeMillis()

  protected def timeTaken[T](metricName: String)(body: => T): T = {
    val startTime = getTimeMillis
    val result = body
    val endTime = getTimeMillis
    val timeTaken = math.max(endTime - startTime, 0)
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


