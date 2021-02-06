package com.keks.sf.soap


/**
  * Metadata about loading process on executor.
  * The same metric class could be used several times to keep loading history.
  *
  * @param offset offset value
  * @param isDone is all data was loaded
  * @param queryLocator last query locator
  * @param loadedRecordsSum sum of loaded records
  * @param loadedRecords number of loaded records
  * @param recordsToLoad number of records that should be loaded
  * @param batchResponseTimeAvg time spent to request and receive one sf batch
  * @param batchSfProcessingTimeAvg time spent to compute one batch by spark
  * @param partitionProcessingTime time spent to one partition
  * @param recordsInSfBatch number of records in one sf batch
  * @param lastProcessedRecordTime time when the last record was processed in this partition
  */
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
