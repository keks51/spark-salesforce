package com.keks.sf.soap

case class ExecutorMetrics(offset: String,
                           isDone: Boolean = false,
                           queryLocator: Option[String] = None,
                           loadedRecordsSum: Int = 0,
                           loadedRecords: Int = 0,
                           recordsToLoad: Int = 0,
                           batchResponseTimeAvg: Long = 0L,
                           batchSfProcessingTimeAvg: Long = 0L,
                           partitionProcessingTime: Long = 0L,
                           recordsInSfBatch: Int = 0,
                           lastProcessedRecordTime: Long = 0L)
