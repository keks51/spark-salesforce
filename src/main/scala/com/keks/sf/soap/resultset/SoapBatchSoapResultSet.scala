package com.keks.sf.soap.resultset

import com.keks.sf.SfTaskMetrics.updateMetrics
import com.keks.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.sf.soap.{SalesforceRecordParser, SfSparkPartition, SoapQueryExecutor}
import com.keks.sf.util.BatchTimeMetrics
import com.keks.sf.{LogSupport, SfOptions, SfResultSet, SfTaskMetrics}
import com.sforce.soap.partner.QueryResult
import com.sforce.ws.bind.XmlObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

import scala.util.Try


class SoapBatchSoapResultSet(sfOptions: SfOptions,
                             firstQueryResult: QueryResult,
                             soql: String,
                             sfRecordsParser: SalesforceRecordParser,
                             sfPartition: SfSparkPartition,
                             sfTaskMetrics: Option[SfTaskMetrics],
                             queryExecutor: SoapQueryExecutor,
                             partitionId: Int,
                             batchTimeMetrics: BatchTimeMetrics) extends SfSoapResultSet(sfOptions = sfOptions,
                                                                                         firstQueryResult = firstQueryResult,
                                                                                         soql = soql,
                                                                                         sfPartition = sfPartition,
                                                                                         partitionId) {

  sfTaskMetrics.foreach(updateMetrics(_, recordsInBatch, 1))

  def hasNext: Boolean = {
    val isLast = currentQueryResult.isDone
    if (isLast) sfPartition.setIsDone()

    val next =
      if (recordsInBatch - 1 == batchCursor) {
        !isLast
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
      sfTaskMetrics.foreach(updateMetrics(_, recordsInBatch, batchCounter))
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

      (true, null.asInstanceOf[InternalRow])
    } else {
      processedRecordCount += 1
      batchCursor += 1
      val isEmpty = isBatchEmpty(batchCursor)
      (false, sfRecordsParser.parse(currentBatch(batchCursor), isEmpty))
    }
  }

}

object SoapBatchSoapResultSet extends LogSupport {

  // TODO get offset column from sfOptions
  def apply(sfOptions: SfOptions,
            soql: String,
            requiredColsBySchemaOrdering: Array[(String, DataType)],
            inputMetricsOpt: Option[SfTaskMetrics],
            queryExecutor: SoapQueryExecutor,
            partitionId: Int,
            offsetColName: String,
            sfSparkPartition: SfSparkPartition): SfResultSet = {
    val batchTimeMetrics = new BatchTimeMetrics()
    val firstQueryResult = queryExecutor.tryToQuery(soql, batchCounter = 0, None, None)

    val sfRecords = firstQueryResult.getRecords

    if (sfRecords.isEmpty) {
      SfResultSet.empty
    } else {
      val sfRecordHead: Array[XmlObject] = convertXmlObjectToXmlFieldsArray(sfRecords.head)
      val headRowColNames = sfRecordHead.map(_.getName.getLocalPart)
      val sfRecordParser = SalesforceRecordParser(headRowColNames, requiredColsBySchemaOrdering, sfSparkPartition, offsetColName, sfSparkPartition.offsetValueIsString)
      new SoapBatchSoapResultSet(sfOptions,
                                 firstQueryResult,
                                 soql,
                                 sfRecordParser,
                                 sfSparkPartition,
                                 inputMetricsOpt,
                                 queryExecutor,
                                 partitionId,
                                 batchTimeMetrics)
    }

  }

}

