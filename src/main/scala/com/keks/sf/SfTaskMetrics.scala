package com.keks.sf

import com.keks.sf.util.LetterTimeUnit.{M, S}
import com.keks.sf.util.{DurationPrinter, FieldModifier}
import org.apache.spark.executor.InputMetrics


case class SfTaskMetrics(inputMetrics: InputMetrics) {

  private var time: Long = 0

  private val incRecordsReadPublic = FieldModifier
    .setMethodAccessible(inputMetrics, "incRecordsRead", classOf[Long])

  def incRecordsRead(v: Long): Unit = {
    incRecordsReadPublic.invoke(inputMetrics,  long2Long(v))
  }

  def commitTime(): Unit = { time = System.currentTimeMillis }

  def getCommittedTime = time

}

object SfTaskMetrics extends LogSupport {

  def updateMetrics(inputMetrics: SfTaskMetrics, loadedRecordsNumber: Int, batchNumber: Int): Unit = {
    inputMetrics.incRecordsRead(loadedRecordsNumber)
    if (batchNumber % 100 == 0) {
      info(s"Last average 100 batch time is '${DurationPrinter.print[M, S](System.currentTimeMillis - inputMetrics.getCommittedTime)}'")
      inputMetrics.commitTime()
    }
  }

}
