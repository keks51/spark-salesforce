package com.keks.sf.soap.v2.streaming

import com.keks.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.sf.soap.resultset.SfSoapResultSet
import com.keks.sf.soap.{SalesforceRecordParser, SfSparkPartition, SoapQueryExecutor}
import com.keks.sf.util.{BatchTimeMetrics, SoqlUtils}
import com.keks.sf.{LogSupport, SerializableConfiguration, SfOptions, SfResultSet}
import com.sforce.soap.partner.QueryResult
import com.sforce.ws.bind.XmlObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.mule.tools.soql.SOQLParserHelper

import scala.util.Try


class StreamingSoapResultSet(sfOptions: SfOptions,
                             firstQueryResult: QueryResult,
                             soql: String,
                             sfRecordsParser: SalesforceRecordParser,
                             sfPartition: SfSparkPartition,
                             queryExecutor: SoapQueryExecutor,
                             partitionId: Int,
                             checkPointRW: SfPartitionsReadWrite,
                             batchTimeMetrics: BatchTimeMetrics) extends SfSoapResultSet(sfOptions = sfOptions,
                                                                                         firstQueryResult = firstQueryResult,
                                                                                         soql = soql,
                                                                                         sfPartition = sfPartition,
                                                                                         partitionId) {
  val batchBoundNumber = sfOptions.streamingMaxBatches
  sfPartition.setRecordsToLoad(globalNumberOfRecords)

  def hasNext: Boolean = {
    sfPartition.setLastProcessedRecordTime(getCurrentTime)
    val isLast = currentQueryResult.isDone
    if (isLast) sfPartition.setIsDone()

    val next =
      if (recordsInBatch - 1 == batchCursor) {
        val isBound = batchBoundNumber == batchCounter
        if (isBound) {
          info(s"Partition: $partitionId. Loaded last batch of limit: '$batchBoundNumber'. Stopping loading and saving query locator.")
          sfPartition.setQueryLocator(currentQueryResult.getQueryLocator)
        }
        !(isLast || isBound)
      } else {
        true
      }
    if (!next) {
      sfPartition.updateMetrics(
        processedRecordCount,
        Try(sparkBatchProcessingTimes.sum / sparkBatchProcessingTimes.length).getOrElse(0L),
        batchTimeMetrics.getBatchResponseTimeAvg,
        partitionProcessingTime = getCurrentTime - partitionProcessingStartTime,
        recordsInSfBatch = recordsInBatch)

      checkPointRW.writeExecutorSfPartition(partitionId, sfPartition)
      info(s"PartitionId: '$partitionId'. Query '$printSoql' finished. Loaded: $processedRecordCount in '$batchCounter' batches")
    }
    next
  }

  def getRow: (Boolean, InternalRow) = {
    if (isBatchEmpty(batchCursor)) {
      sparkBatchProcessingTimes += math.max(System.currentTimeMillis() - sparkBatchProcessingStartTime, 0)
      batchCounter += 1
      currentQueryResult = batchTimeMetrics.withBatchResponseTimeTaken(
        queryExecutor.tryToQuery(
          soql,
          batchCounter,
          Some(currentQueryResult.getQueryLocator),
          Some(sfPartition.getOffset)))
      currentBatch = currentQueryResult.getRecords
      recordsInBatch = currentBatch.length

      batchCursor = -1
      info(s"PartitionId: '$partitionId'. Loaded batch: '$batchCounter' of '$maxBatchNumber'. Sum of records: '$processedRecordCount'")
      sparkBatchProcessingStartTime = System.currentTimeMillis()
    }

    if (recordsInBatch == 0) {
      sfPartition.setIsDone()
      sfPartition.updateMetrics(
        processedRecordCount,
        Try(sparkBatchProcessingTimes.sum / sparkBatchProcessingTimes.length).getOrElse(0L),
        batchTimeMetrics.getBatchResponseTimeAvg,
        partitionProcessingTime = getCurrentTime - partitionProcessingStartTime,
        recordsInSfBatch = recordsInBatch)
      checkPointRW.writeExecutorSfPartition(partitionId, sfPartition)
      (true, null.asInstanceOf[InternalRow])
    } else {
      processedRecordCount += 1
      batchCursor += 1
      val isEmpty = isBatchEmpty(batchCursor)
      (false, sfRecordsParser.parse(currentBatch(batchCursor), isEmpty))
    }
  }

}

object StreamingSoapResultSet extends LogSupport {

  def apply(sfOptions: SfOptions,
            soql: String,
            requiredColsBySchemaOrdering: Array[(String, DataType)],
            queryExecutor: SoapQueryExecutor,
            partitionId: Int,
            hadoopConf: SerializableConfiguration,
            offsetColName: String,
            checkpointLocation: String,
            previousSfStreamingPartition: SfSparkPartition): SfResultSet = {
    val batchTimeMetrics = new BatchTimeMetrics()
    val previousExecutorMetrics = previousSfStreamingPartition.executorMetrics
    val firstQueryResult = previousExecutorMetrics.queryLocator.map { queryLocator =>
      val previousOffset = previousExecutorMetrics.offset
      val printSoql = SoqlUtils.printSOQL(SOQLParserHelper.createSOQLData(soql), sfOptions.isSelectAll)
      info(s"PartitionId: '$partitionId'. Continue loading from previous locator: '$queryLocator' with offset: '$previousOffset'. Soql: '$printSoql'")
      batchTimeMetrics
        .withBatchResponseTimeTaken(queryExecutor.tryToQuery(
          soql,
          batchCounter = 0,
          queryLocatorOpt = Some(queryLocator),
          lastOffsetOpt = Some(previousOffset)))
    }.getOrElse(batchTimeMetrics.withBatchResponseTimeTaken(queryExecutor.tryToQuery(soql, batchCounter = 0, None, None)))

    val sfRecords = firstQueryResult.getRecords
    val checkPointRW: SfPartitionsReadWrite = SfPartitionsReadWrite(checkpointLocation, hadoopConf.value)
    if (sfRecords.isEmpty) {
      previousSfStreamingPartition.setIsDone()
      checkPointRW.writeExecutorSfPartition(partitionId, previousSfStreamingPartition)
      SfResultSet.empty
    } else {
      val sfRecordHead: Array[XmlObject] = convertXmlObjectToXmlFieldsArray(sfRecords.head)
      val headRowColNames = sfRecordHead.map(_.getName.getLocalPart)
      val sfRecordParser = SalesforceRecordParser(
        headRowColNames,
        requiredColsBySchemaOrdering,
        previousSfStreamingPartition,
        offsetColName,
        previousSfStreamingPartition.offsetValueIsString)
      new StreamingSoapResultSet(sfOptions,
                                 firstQueryResult,
                                 soql,
                                 sfRecordParser,
                                 previousSfStreamingPartition,
                                 queryExecutor,
                                 partitionId,
                                 checkPointRW,
                                 batchTimeMetrics)
    }

  }

}
