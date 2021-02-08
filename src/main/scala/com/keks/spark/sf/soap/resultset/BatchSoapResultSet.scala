package com.keks.spark.sf.soap.resultset

import com.keks.spark.sf.SfTaskMetrics.updateMetrics
import com.keks.spark.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.spark.sf.soap.{SalesforceRecordParser, SfSparkPartition, SoapQueryExecutor}
import com.keks.spark.sf.util.{BatchTimeMetrics, UniqueQueryId}
import com.keks.spark.sf.{LogSupport, SfOptions, SfResultSet, SfTaskMetrics}
import com.sforce.soap.partner.QueryResult
import com.sforce.ws.bind.XmlObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

import scala.util.Try


/**
  * Like Jdbc result set for getting salesforce data
  * while processing data by Spark Batch.
  *
  * @param sfOptions        spark query options
  * @param firstQueryResult first soap query result
  * @param soql             query soql
  * @param sfRecordsParser  salesforce record parser
  * @param sfPartition      executor partition
  * @param sfTaskMetrics    specific metric for Spark Datasource Api V1
  * @param queryExecutor    Soap query executor
  * @param batchTimeMetrics batch metrics container
  */
class BatchSoapResultSet(sfOptions: SfOptions,
                         firstQueryResult: QueryResult,
                         soql: String,
                         sfRecordsParser: SalesforceRecordParser,
                         sfPartition: SfSparkPartition,
                         sfTaskMetrics: Option[SfTaskMetrics],
                         queryExecutor: SoapQueryExecutor,
                         batchTimeMetrics: BatchTimeMetrics)
                        (implicit queryId: UniqueQueryId) extends SfSoapResultSet(sfOptions = sfOptions,
                                                                                  firstQueryResult = firstQueryResult,
                                                                                  soql = soql,
                                                                                  sfPartition = sfPartition) {

  sfTaskMetrics.foreach(updateMetrics(_, recordsInBatch, 1))

  /**
    * If batch query result isDone and last processed
    * record is the last record in batch then hasNext = false.
    * If hasNext = false then executor metrics should be updated.
    */
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

      info(s"PartitionId: '$partitionId'. Query '$prettySoql' finished. Loaded: $processedRecordCount in '$batchCounter' batches")
    }
    next
  }

  /**
    * Getting record data as (isLast record, spark row)
    * If all records were processed then next batch should be requested.
    * Sometimes due to connection issues like 'Invalid query locator' new requested batch
    * during retries can be empty and an additional check should be applied.
    */
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

object BatchSoapResultSet extends LogSupport {

  /**
    * Creating SoapBatchSoapResultSet.
    * Sometimes data queried by partition SOQL can be empty.
    * For example, partition SOQL is: 'SELECT Id FROM User WHERE (Age >= 10 AND Age <= 20)' and
    * returned batch can be empty if no data exists for this period.
    * Creating SalesforceRecordParser function.
    *
    * @param sfOptions                    spark query options
    * @param soql                         partition soql
    * @param requiredColsBySchemaOrdering result schema
    * @param inputMetricsOpt              specific metric for Spark Datasource Api V1
    * @param queryExecutor                Soap query executor
    * @param sfSparkPartition             executor partition
    */
  def apply(sfOptions: SfOptions,
            soql: String,
            requiredColsBySchemaOrdering: Array[(String, DataType)],
            inputMetricsOpt: Option[SfTaskMetrics],
            queryExecutor: SoapQueryExecutor,
            sfSparkPartition: SfSparkPartition)(implicit queryId: UniqueQueryId): SfResultSet = {
    val batchTimeMetrics = new BatchTimeMetrics()
    val firstQueryResult = queryExecutor.tryToQuery(soql, batchCounter = 0, None, None)

    val sfRecords = firstQueryResult.getRecords

    if (sfRecords.isEmpty) {
      SfResultSet.empty
    } else {
      val sfRecordHead: Array[XmlObject] = convertXmlObjectToXmlFieldsArray(sfRecords.head)
      val headRowColNames = sfRecordHead.map(_.getName.getLocalPart)
      val sfRecordParser = SalesforceRecordParser(
        headRowColNames,
        requiredColsBySchemaOrdering,
        sfSparkPartition,
        sfOptions.offsetColumn,
        sfSparkPartition.offsetValueIsString)
      new BatchSoapResultSet(sfOptions,
                             firstQueryResult,
                             soql,
                             sfRecordParser,
                             sfSparkPartition,
                             inputMetricsOpt,
                             queryExecutor,
                             batchTimeMetrics)
    }

  }

}

