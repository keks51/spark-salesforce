package com.keks.spark.sf.soap.resultset

import com.keks.spark.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.spark.sf.soap.v2.streaming.SfPartitionsReadWrite
import com.keks.spark.sf.soap.{SalesforceRecordParser, SfSparkPartition, SoapQueryExecutor}
import com.keks.spark.sf.util.{BatchTimeMetrics, SoqlUtils, UniqueQueryId}
import com.keks.spark.sf.{LogSupport, SerializableConfiguration, SfOptions, SfResultSet}
import com.sforce.soap.partner.QueryResult
import com.sforce.ws.bind.XmlObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.mule.tools.soql.SOQLParserHelper

import scala.util.Try


/**
  * Like Jdbc result set for getting salesforce data
  * while processing data by Spark Streaming.
  * Before requesting new Soap batch and when hasNext = false executor metrics
  * should be saved to checkPoint dir.
  *
  * @param sfOptions        spark query options
  * @param firstQueryResult first soap query result
  * @param soql             query soql
  * @param sfRecordsParser  salesforce record parser
  * @param sfPartition      executor partition
  * @param queryExecutor    Soap query executor
  * @param checkPointRW     util class to save metrics
  * @param batchTimeMetrics batch metrics container
  */
class StreamingSoapResultSet(sfOptions: SfOptions,
                             firstQueryResult: QueryResult,
                             soql: String,
                             sfRecordsParser: SalesforceRecordParser,
                             sfPartition: SfSparkPartition,
                             queryExecutor: SoapQueryExecutor,
                             checkPointRW: SfPartitionsReadWrite,
                             batchTimeMetrics: BatchTimeMetrics)
                            (implicit uniqueQueryId: UniqueQueryId) extends SfSoapResultSet(sfOptions = sfOptions,
                                                                                         firstQueryResult = firstQueryResult,
                                                                                         soql = soql,
                                                                                         sfPartition = sfPartition) {

  /* In streaming processing executor finishes processing if maxBatch number bound is reached. */
  val batchBoundNumber = sfOptions.streamingMaxBatches
  sfPartition.setRecordsToLoad(globalNumberOfRecords)

  /**
    * If batch query result isDone and last processed
    * record is the last record in batch then hasNext = false.
    * Also if batchBoundNumber is reached then hasNext = false.
    * If hasNext = false then executor metrics should be updated.
    */
  def hasNext: Boolean = {
    sfPartition.setLastProcessedRecordTime(getCurrentTime)
    val isLast = currentQueryResult.isDone
    if (isLast) sfPartition.setIsDone()

    val next =
      if (recordsInBatch - 1 == batchCursor) {
        val isBound = batchBoundNumber == batchCounter
        if (isBound) {
          infoQ(s"Partition: $partitionId. Loaded last batch of limit: '$batchBoundNumber'. Stopping loading and saving query locator.")
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
      infoQ(s"PartitionId: '$partitionId'. Query '$prettySoql' finished. \n   Loaded: '$processedRecordCount' records in '$batchCounter' batches")
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
      infoQ(s"PartitionId: '$partitionId'. Loaded batch: '$batchCounter' of '$maxBatchNumber'. Sum of records: '$processedRecordCount'")
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

  /**
    * Creating StreamingSoapResultSet.
    * Sometimes data queried by partition SOQL can be empty.
    * For example, partition SOQL is: 'SELECT Id FROM User WHERE (Age >= 10 AND Age <= 20)' and
    * returned batch can be empty if no data exists for this period.
    * While streaming previous partition can be executed again, then query locator is used.
    * Creating SalesforceRecordParser function.
    *
    * @param sfOptions                    spark options wrapped in SfOptions
    * @param soql                         partition soql
    * @param requiredColsBySchemaOrdering result schema
    * @param queryExecutor                Soap query executor
    * @param hadoopConf                   hadoop conf
    * @param checkpointLocation           streaming checkpoint dir location
    * @param previousSfStreamingPartition executor partition
    */
  def apply(sfOptions: SfOptions,
            soql: String,
            requiredColsBySchemaOrdering: Array[(String, DataType)],
            queryExecutor: SoapQueryExecutor,
            hadoopConf: SerializableConfiguration,
            checkpointLocation: String,
            previousSfStreamingPartition: SfSparkPartition)
           (implicit uniqueQueryId: UniqueQueryId): SfResultSet = {
    val partitionId = previousSfStreamingPartition.id
    val batchTimeMetrics = new BatchTimeMetrics()
    val previousExecutorMetrics = previousSfStreamingPartition.executorMetrics
    val firstQueryResult = previousExecutorMetrics.queryLocator.map { queryLocator =>
      val previousOffset = previousExecutorMetrics.offset
      val printSoql = SoqlUtils.printSOQL(SOQLParserHelper.createSOQLData(soql), sfOptions.isSelectAll)
      infoQ(s"PartitionId: '$partitionId'. Continue loading from previous locator: '$queryLocator' with offset: '$previousOffset'. Soql: '$printSoql'")
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
        sfOptions.offsetColumn,
        previousSfStreamingPartition.offsetValueIsString)
      new StreamingSoapResultSet(sfOptions,
                                 firstQueryResult,
                                 soql,
                                 sfRecordParser,
                                 previousSfStreamingPartition,
                                 queryExecutor,
                                 checkPointRW,
                                 batchTimeMetrics)
    }

  }

}
