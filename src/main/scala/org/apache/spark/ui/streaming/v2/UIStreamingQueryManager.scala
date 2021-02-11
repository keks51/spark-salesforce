package org.apache.spark.ui.streaming.v2

import com.keks.spark.sf.LogSupport
import com.keks.spark.sf.soap.SfSparkPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.streaming.v2.TransformationStatus.TransformationStatus

import java.util.concurrent.atomic.AtomicInteger


object UIStreamingQueryManager extends LogSupport {

  val SF_STREAMING_TAB = "SfStreaming"

  private val idCounter = new AtomicInteger(0)

  private var sfUiStreamingTabOpt: Option[SfUIStreamingTab] = None
  private var sfUiStreamingPageOpt: Option[SfStreamingPage] = None

  def init()(implicit spark: SparkSession): Unit = {
    (sfUiStreamingTabOpt, spark.sparkContext.ui) match {
      case (Some(_), _) =>
      case (None, None) =>
        info(s"Cannot enable SfStreaming ui tab because spark ui is explicitly disabled.")
      case (None, Some(sparkUI)) =>
        synchronized {
          if (sfUiStreamingTabOpt.isEmpty) {
            info(s"Starting SfStreaming ui tab")
            val tab = SfUIStreamingTab(sparkUI)
            sfUiStreamingTabOpt = Some(tab)
            val page = SfStreamingPage(tab)
            sfUiStreamingPageOpt = Some(page)
            tab.attachPage(page)
            sparkUI.attachTab(tab)
          }
        }
    }
  }

  def getUI(queryName: String,
            streamingStartTime: Long,
            soql: String,
            offsetCol: String,
            initialOffset: Option[String],
            maxOffset: Option[String],
            queryConf: QueryConf)(implicit spark: SparkSession): UIStreamingQueryManager = {
    init()
    UIStreamingQueryManager(sfUiStreamingPageOpt.get, queryName, idCounter.incrementAndGet(), streamingStartTime, soql, offsetCol, initialOffset, maxOffset, queryConf)
  }

}

case class UIStreamingQueryManager(page: SfStreamingPage,
                                   queryName: String,
                                   queryID: Int,
                                   streamingStartTime: Long,
                                   soql: String,
                                   offsetCol: String,
                                   initialOffset: Option[String],
                                   maxOffset: Option[String],
                                   queryConf: QueryConf) {
  page.addQuery(queryID, QueryUiData(queryName, soql, streamingStartTime, streamingStartTime, offsetCol, initialOffset,maxOffset, queryConf = queryConf))

  private def update(func: QueryUiData => QueryUiData): Unit = {
    page.updateQueryData(queryID, func(page.getQueryData(queryID)))
  }

  def updateStatus(status: TransformationStatus): Unit = update { current => current.copy(status = status) }

  def updateMaxOffset(offset: Option[String]): Unit = update { current => current.copy(maxOffset = offset) }

  def update(soql: String, currentOffset: String, status: TransformationStatus): Unit = update { current =>
    current.copy(soql = soql, status = status)
  }

  def updateMetrics(loadedRecords: Int,
                    microBatchesCount: Int,
                    partitions: Array[SfSparkPartition]): Unit = update { current =>
    current.copy(loadedRecords = loadedRecords, microBatchesCount = microBatchesCount, partitions = partitions)
  }

  def updateLastMicroBatchTime(time: Long): Unit = update { current => current.copy(lastMicroBatchTime = time) }

}
