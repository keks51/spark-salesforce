package org.apache.spark.ui.streaming.v2

import com.keks.sf.soap.ExecutorMetrics
import com.keks.sf.util.DurationPrinter
import com.keks.sf.util.LetterTimeUnit._
import org.apache.spark.ui.streaming.v2.TransformationStatus.TransformationStatus
import org.apache.spark.ui.{SparkUITab, UIUtils, WebUIPage}

import javax.servlet.http.HttpServletRequest
import scala.collection.concurrent.TrieMap
import scala.xml.{Node, Unparsed}


// TODO check with prefix instead of ""
case class SfStreamingPage(sparkUITab: SparkUITab) extends WebUIPage("") {

  private val ID = "Id"
  private val QUERY_NAME = "Query name"
  private val SOQL = "SOQL"
  private val STATUS = "Status"
  private val QUERY_METRICS = "Query Metrics"
  private val PARTITIONS = "Partitions"
  private val QUERY_CONF = "QUERY_CONF"
  private val tablesHeader = Seq(
    ID,
    QUERY_NAME,
    SOQL,
    QUERY_METRICS,
    PARTITIONS,
    QUERY_CONF,
    STATUS)
  private val tables = TrieMap.empty[Int, QueryUiData]
  val refreshIntervalInMs = 0
  private val jsRefreshPageByInterval =
    if (refreshIntervalInMs > 0) "setInterval(\"window.location.reload(true);\"," + refreshIntervalInMs + ");" else ""

  def updateQueryData(id: Int, queryUiData: QueryUiData): Unit = {
    tables.update(id, queryUiData)
  }

  def getQueryData(id: Int): QueryUiData = {
    tables(id)
  }

  def addQuery(id: Int, queryUiData: QueryUiData): Unit = {
    tables += (id -> queryUiData)
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val tablesProgress = UIUtils.listingTable(
      tablesHeader,
      queryDataMap,
      tables,
      sortable = true,
      stripeRowsWithCss = false)

    val content =
      <h4>Progress</h4>
        <div>
          {tablesProgress}
        </div>
        <script>
          {Unparsed(jsRefreshPageByInterval)}
        </script>
    UIUtils.headerSparkPage(request, "Table transformations", content, sparkUITab)
  }

  private def queryDataMap(queryData: (Int, QueryUiData)): Seq[Node] = {
    val (id,
    QueryUiData(
    queryName,
    soql,
    streamingStartTime,
    lastMicroBatchTime,
    offsetCol,
    initialOffset,
    maxOffset,
    partitions,
    loadedRecords,
    microBatchesCount,
    status,
    queryConf)) = queryData
    val timePrinter =  DurationPrinter.print[H, M, S, MS](_)

    val partitionsStr = partitions.map { sfStreamingPartition =>
      val clause = sfStreamingPartition.getWhereClause.replaceAll("AND", "AND\n   ")
      val ExecutorMetrics(offset, _, _, loadedRecordsSum, _, recordsToLoad, batchResponseTimeAvg, batchSfProcessingTimeAvg, partitionProcessingTime, recordsInSfBatch, lastProcessedRecordTime) = sfStreamingPartition.executorMetrics
      val batchResponseTimeAvgStr = timePrinter(batchResponseTimeAvg)
      val batchSfProcessingTimeAvgStr = timePrinter(batchSfProcessingTimeAvg)
      val partitionProcessingTimeStr = timePrinter(partitionProcessingTime)
      val updateLastSfDataRequestedTimeStr = new java.sql.Timestamp(lastProcessedRecordTime)
      s"""ID: ${sfStreamingPartition.id}
         |Clause: '$clause'
         |offset: '$offset'
         |RecordsInLastOneSfBatch: $recordsInSfBatch
         |LoadedRecords: $loadedRecordsSum
         |recordsToLoad: $recordsToLoad
         |SfBatchResponseTimeAvg: $batchResponseTimeAvgStr
         |SparkSfBatchProcessingTimeAvg: $batchSfProcessingTimeAvgStr
         |PartitionProcessingTime: $partitionProcessingTimeStr
         |LastSfDataRequestedTime: $updateLastSfDataRequestedTimeStr""".stripMargin
    }

    val queryMetrics =
      s"""StreamingStartTime: ${new java.sql.Timestamp(streamingStartTime)}
         |LastMicroBatchTime: ${new java.sql.Timestamp(lastMicroBatchTime)}
         |OffsetCol: $offsetCol
         |initialOffset: ${initialOffset.getOrElse(None)}
         |maxOffset: ${maxOffset.getOrElse(None)}
         |loadedRecords: $loadedRecords
         |microBatchesCount: $microBatchesCount""".stripMargin

    <tr>
      <td style="width: 3%">
        #
        {id}
      </td>
      <td style="width: 5%; word-wrap: break-word;">
        {queryName}
      </td>
      <td style="width: 15%; word-wrap: break-word;">
        {soql}
      </td>
      <td style="width: 17%; word-wrap: break-word;">
        <pre>{queryMetrics}</pre>
      </td>
      <td style="width: 25%; word-wrap: break-word;">
        <pre>{partitionsStr.mkString("\n\n")}</pre>
      </td>
      <td style="width: 15%; word-wrap: break-word;">
        <pre>{queryConf.toString}</pre>
      </td>
      <td style={s"${jsBgColorForColumn(status)} width: 10%; word-wrap: break-word;"}>
        {status}
      </td>
    </tr>
  }

  private def jsBgColorForColumn(status: TransformationStatus): String = {
    status match {
      case TransformationStatus.STARTING => "background-color: #95ff95;"
      case TransformationStatus.PROCESSING_DATA => "background-color: #95ff95;"
      case TransformationStatus.WAITING_DATA => "background-color: #E3DA27;"
      case TransformationStatus.FAILED => "background-color: #ff6666;"
      case TransformationStatus.FINISHED => "background-color: #4EA94E;"
      case _ => "background-color: inherit;"
    }
  }

}
