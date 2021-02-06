package com.keks.sf.soap.v2.streaming

import com.keks.sf.soap.v2.SoapDataSourceReaderV2
import com.keks.sf.soap.v2.streaming.SoapDataSourceStreamingReaderV2.printPartitions
import com.keks.sf.soap.{SfSparkPartition, SfStreamingPartitions}
import com.keks.sf.util.LetterTimeUnit.{H, M, MS, S}
import com.keks.sf.util._
import com.keks.sf.{LogSupport, PartitionSplitter, SfOptions, SfSoapConnection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ui.streaming.v2.{QueryConf, TransformationStatus, UIStreamingQueryManager}
import org.mule.tools.soql.SOQLParserHelper

import java.util.Optional
import scala.collection.JavaConverters._


/**
  * Spark Streaming DataSource Api V2 implementation.
  * Supporting elimination of unneeded columns and filtration using selected predicates.
  * On Driver side:
  * 1) getting the 'offsetColumn' from configuration or taking default (Systemmodstamp).
  * 2) getting the 'initial offset' from conf or salesforce table.
  * 3) getting the 'last offset' from conf or salesforce table.
  * 4) Splitting query in partitions by 'loadNumPartitions' for example '3' and adding an 'order by' clause
  * "select id, name from account where (name = 'alex') AND (age >=  0 AND age < 10) ORDER BY age"
  * "select id, name from account where (name = 'alex') AND (age >= 10 AND age < 20) ORDER BY age"
  * "select id, name from account where (name = 'alex') AND (age >= 20 AND age < 30) ORDER BY age"
  * 5) executing each query on Executor in parallel.
  * Each executor loads data until 'streamingMaxRecordsInPartition' threshold. Then the micro batch job finishes,
  * and the executor's metrics like:
  * - last batch offset value
  * - next batch query locator
  * - number of loaded records
  * - was all data loaded or not
  * are saved in the checkPoint dir.
  * Driver loads these metrics from the checkpoint dir.
  * If all partitions still have data to load then only lower bounds offsets are changed.
  * For example '(SystemModstamp >= 2011-01-01T00:00:00.000Z AND... is changed to '(SystemModstamp >= 'last offset value' AND...'
  * If some partitions loaded all available data then all partitions are recreated:
  * before:
  * 'age >= 0 and age < 10'  finished
  * 'age >= 10 and age < 20' finished
  * 'age >= 20 and age < 30' not finished. last value is 12
  * recreated to:
  * 'age >= 12 and age < 14'
  * 'age >= 14 and age < 17'
  * 'age >= 17 and age < 20'
  * This algorithm is repeated until all data between 'initial offset' and 'end offset' is loaded.
  * Recreating partitions approach allow to load large and skewed tables with minimum of cluster resources.
  * If option 'streamingLoadAvailableData' is true then spark streaming job is finished.
  * Else new data is polling each 'streamingAdditionalWaitWhenIncrementalLoading' and the number of partitions is calculated by:
  * 'math.min(sfOptions.sfLoadNumPartitions, math.ceil(newRecords / sfOptions.streamingMaxRecordsInPartition))'
  *
  * @param sfOptions          spark options wrapped in SfOptions
  * @param predefinedSchema   .schema(...)
  * @param checkpointLocation streaming checkpoint dir location
  * @param spark              spark session
  */
class SoapDataSourceStreamingReaderV2(sfOptions: SfOptions,
                                      predefinedSchema: Option[StructType],
                                      var checkpointLocation: String)(implicit spark: SparkSession,
                                                                      queryId: UniqueQueryId,
                                                                      sfSoapConnection: SfSoapConnection,
                                                                      parsedSoqlData: ParsedSoqlData)
  extends SoapDataSourceReaderV2(sfOptions, predefinedSchema)
    with DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with MicroBatchReader
    with LogSupport {

  /* partition manager */
  lazy val sfPartitionsRW = SfPartitionsReadWrite(checkpointLocation, hadoopConf) // should be lazy
  /* on start partitions */
  lazy val initPartitions: Array[SfSparkPartition] = initializeStreaming
  /**
    * Max offset from all partitions.
    * For example on start partitions were:
    * 'age >= 10 AND age < 20'
    * 'age >= 20 AND age < 30'
    * 'age >= 30 AND age < 40'
    * then maxUpperBoundOffset is '40'
    */
  var maxOffset: Option[String] = None
  /* additional streaming UI */
  var queryUIOpt: Option[UIStreamingQueryManager] = None
  /* instead of using spark streaming internal offset, just counter is used */
  var endOffsetMockCounter: Int = 0
  /* Number of records loaded during streaming */
  var loadedRecordsSum: Int = 0
  /* Number of spark micro jobs executed during streaming */
  var microBatchesCount = 0
  /* should be executed during next spark micro job */
  var toProcessPartitions: Array[SfSparkPartition] = Array.empty
  /* all data was loaded and waiting new? */
  var waitingForNewData: Boolean = false
  /* just because spark streaming implementation requires it*/
  var sfStartOffset: Option[SfOffset] = None

  /**
    * Defining partitions on start.
    * Deleting executor partitions on start to prevent incorrect behavior.
    * If streaming was stopped and is started again then partitions data is read
    * to continue loading.
    * Starting query UI if query name is defined.
    *
    * @return
    */
  def initializeStreaming = {
    info(s"Initializing. Finding partitions from previous loading.")
    sfPartitionsRW.deleteExecutorsPartitionsDir()
    val (partitions, lastOffset) = sfPartitionsRW
      .readDriverSfPartitions
      .map { previousPartitions =>
        val SfStreamingPartitions(partitions, maxUpperBoundOffset) = previousPartitions
        if (partitions.nonEmpty) {
          info(s"Loaded previous partitions: \n${partitions.map(e => s"ID: '${e.id}'. ${e.getWhereClause}. Finished: ${e.executorMetrics.isDone}").mkString("\n")}")
        } else {
          info(s"No previous partition. Continue polling Salesforce with latest offset: '${maxUpperBoundOffset.getOrElse("None")}'")
        }
        info(s"Continue streaming, max offset is: '$maxUpperBoundOffset'")
        (previousPartitions.partitions, maxUpperBoundOffset)
      }
      .getOrElse {
        info(s"No previous partitions. Start loading from the the first record")
        val partitions: Array[SfSparkPartition] = initialPartitions
        sfPartitionsRW.writeDriverSfPartitions(SfStreamingPartitions(partitions, initialEndOffset))
        (partitions, initialEndOffset)
      }

    maxOffset = lastOffset
    queryUIOpt = sfOptions.streamingQueryNameOpt.map { name =>
      UIStreamingQueryManager
        .getUI(queryName = name,
               System.currentTimeMillis,
               soql = SoqlUtils.printSOQL(soqlToQuery, parsedSoqlData.isSelectAll),
               offsetCol = parsedSoqlData.offsetColName,
               initialOffset = initialStartOffset,
               maxOffset = maxOffset,
               queryConf = QueryConf(sfOptions))
    }
    partitions.sortBy(_.id)
  }

  /**
    * Sending partitions to executors
    */
  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    queryUIOpt.foreach(_.updateStatus(TransformationStatus.PROCESSING_DATA))
    queryUIOpt.foreach(_.updateLastMicroBatchTime(System.currentTimeMillis))
    microBatchesCount += 1

    println(s"isNewData: $waitingForNewData")
    val printSoql = SoqlUtils.printSOQL(soqlToQuery, isSelectAll)
    debug(s"2Salesforce query is: $printSoql")
    toProcessPartitions.map { streamingPartition =>
      new StreamingSoapPartitionV2(
        soqlFromDriver = soqlToQuery,
        sfOptions = sfOptions,
        rowSchema = rowSchema,
        sfSparkPartition = streamingPartition,
        checkpointLocationOpt = checkpointLocation,
        hdfsConf = serializableHadoopConf)
    }.toList.asInstanceOf[List[InputPartition[InternalRow]]].asJava
  }

  /**
    * Setting offset for streaming query.
    * Actually it is just a mock.
    *
    * @param start start offset
    * @param `end` end offset
    */
  override def setOffsetRange(start: Optional[Offset], `end`: Optional[Offset]): Unit = {
    debug(s"SetOffsetRange START: $start")
    debug(s"SetOffsetRange END: ${`end`}")
    if (`end`.isPresent) {
      debug("SetOffsetRange moving end to start")
      sfStartOffset = Some(SfOffset(`end`.get()))
    }
  }

  /**
    * Getting first streaming offset.
    * Just a mock.
    */
  override def getStartOffset: Offset = {
    debug("GetStartOffset")
    throw new RuntimeException("Start offset is not defined")
  }

  /**
    * Getting processed partitions, by reading executors metadata.
    * If no data is available then using initial partitions.
    */
  def getProcessedPartitions = sfPartitionsRW
    .readExecutorPartitions
    .orElse(sfPartitionsRW.readDriverSfPartitions.map(_.partitions))
    .getOrElse(initPartitions)

  override def getEndOffset: Offset = {
    initPartitions // just init once on start
    val processedPartitions: Array[SfSparkPartition] = getProcessedPartitions


    queryUIOpt.foreach(_.updateMaxOffset(maxOffset)) // updating ui

    info(s"Recreating partitions. Previous: \n${printPartitions(processedPartitions)}")
    if (!waitingForNewData) loadedRecordsSum = loadedRecordsSum + processedPartitions.map(_.executorMetrics.loadedRecords).sum

    toProcessPartitions = PartitionSplitter.recreateSfSparkPartitions(processedPartitions, sfOptions.sfLoadNumPartitions, offsetColName)
    sfPartitionsRW.writeDriverSfPartitions(SfStreamingPartitions(toProcessPartitions, maxOffset))
    // deleting previous metadata to prevent prevent incorrect behavior
    sfPartitionsRW.deleteExecutorsPartitionsDir()

    // updating UI
    queryUIOpt.foreach(_.updateMetrics(loadedRecordsSum, microBatchesCount, toProcessPartitions))

    if (toProcessPartitions.nonEmpty) {
      info(s"Recreated partitions. New: \n${printPartitions(toProcessPartitions)}")
    } else {
      queryUIOpt.foreach(_.updateStatus(TransformationStatus.WAITING_DATA))
      info(s"No new partition")
    }

    if (toProcessPartitions.isEmpty) {
      waitingForNewData = true
      if (sfOptions.streamingLoadAvailableData) {
        info(s"All available data was loaded. Finishing streaming.")
        queryUIOpt.foreach(_.updateStatus(TransformationStatus.FINISHED))
        SfOffset.empty
      } else {
        val soqlWithMaxBound = getQueryWithMaxBound
        val printSoql = SoqlUtils.printSOQL(soqlWithMaxBound, isSelectAll)
        info(s"Checking table '$sfTableName' for new records with query: '$printSoql'")
        val newRecords = sfSoapConnection.countTable(SoqlUtils.convertToCountQuery(soqlWithMaxBound).toSOQLText, sfOptions.isQueryAll)
        lazy val latestOffset = sfSoapConnection.getLatestOffsetFromSF(offsetColName)
        if (newRecords > 0 && latestOffset.isDefined) {
          info(s"Salesforce table '$sfTableName' contains '$newRecords' new records with latest offset: '${latestOffset.get}' for soql: $printSoql")
          val partitionsNumber = if (partitionColType.operationTypeStr == classOf[String].getName) {
            1
          } else {
            math.min(sfOptions.sfLoadNumPartitions, math.ceil(newRecords.toDouble / sfOptions.streamingMaxRecordsInPartition.toDouble).toInt)
          }
          toProcessPartitions = SfUtils.getSparkPartitions(
            offsetColName,
            sfTableName,
            maxOffset,
            latestOffset,
            partitionsNumber,
            isFirstPartitionCondOperatorGreaterAndEqual = false)
          info(s"New partitions are: \n${printPartitions(toProcessPartitions)}")
          maxOffset = latestOffset
          sfPartitionsRW.writeDriverSfPartitions(SfStreamingPartitions(toProcessPartitions, maxOffset))
          waitingForNewData = false
          endOffsetMockCounter += 1
          SfOffset(endOffsetMockCounter.toString)
        } else {
          info(s"No new records for soql: $printSoql")
          val sleep = sfOptions.streamingAdditionalWaitWhenIncrementalLoading
          info(s"All available data was loaded. Sleeping: '${DurationPrinter.print[H, M, S, MS](sleep)}' before fetching next data.")
          Thread.sleep(sleep)
          info(s"Continue loading after sleeping: '${DurationPrinter.print[H, M, S, MS](sleep)}'")
          SfOffset.empty
        }
      }
    } else {
      endOffsetMockCounter += 1
      SfOffset(endOffsetMockCounter.toString)
    }
  }

  /**
    * Getting first streaming offset.
    * Just a mock.
    */
  override def deserializeOffset(json: String) = {
    debug("\n\n\n\n\n\n")
    debug(s"DeserializeOffset: $json")
    SfOffset(json)
  }

  /**
    * Getting first streaming offset.
    * Just a mock.
    */
  override def commit(`end`: Offset): Unit = {}

  override def stop(): Unit = {
    debug("Stopping streaming")
  }

  private def getQueryWithMaxBound: String = {
    maxOffset.map { e =>
      val beforeSoql = SOQLParserHelper.createSOQLData(soqlToQuery)
      val whereCause = s"$offsetColName > ${partitionColType.parseToSfValue(e)}"
      SoqlUtils.addWhereClause(beforeSoql, whereCause, setInParenthesis = true)
    }.getOrElse(SOQLParserHelper.createSOQLData(soqlToQuery))
      .toSOQLText
  }

}

object SoapDataSourceStreamingReaderV2 {

  def printPartitions(sfSparkPartitions: Array[SfSparkPartition]): String = {
    sfSparkPartitions.map(e => s"ID: '${e.id}'. ${e.getWhereClause}. Finished: ${e.executorMetrics.isDone}").mkString("\n")
  }

}
