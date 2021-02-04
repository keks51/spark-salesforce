package com.keks.sf.soap.v2.streaming

import com.keks.sf.soap.v2.SoapDataSourceReaderV2
import com.keks.sf.soap.{SfSparkPartition, SfStreamingPartitions}
import com.keks.sf.util.LetterTimeUnit.{H, M, MS, S}
import com.keks.sf.util._
import com.keks.sf.{LogSupport, PartitionSplitter, SfOptions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ui.streaming.v2.{QueryConf, TransformationStatus, UIStreamingQueryManager}
import org.mule.tools.soql.SOQLParserHelper

import java.util.Optional
import scala.collection.JavaConverters._


class SoapDataSourceStreamingReaderV2(sfOptions: SfOptions,
                                      predefinedSchema: Option[StructType],
                                      var checkpointLocation: String)(implicit spark: SparkSession)
  extends SoapDataSourceReaderV2(sfOptions, predefinedSchema)
    with DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with MicroBatchReader
    with LogSupport {

  lazy val sfPartitionsRW = SfPartitionsReadWrite(checkpointLocation, hadoopConf) // should be lazy
  lazy val initPartitions: Array[SfSparkPartition] = initializeStreaming
  var maxOffset: Option[String] = None
  var queryUIOpt: Option[UIStreamingQueryManager] = None
  var endOffsetMockCounter: Int = 0
  var loadedRecordsSum: Int = 0
  var microBatchesCount = 0
  var toProcessPartitions: Array[SfSparkPartition] = Array.empty
  var waitingForNewData: Boolean = false
  var sfStartOffset: Option[SfOffset] = None

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
               soql = SoqlUtils.printSOQL(soqlWithDefaultFilters, isSelectAll),
               offsetCol = offsetColName,
               initialOffset = initialStartOffset,
               maxOffset = maxOffset,
               queryConf = QueryConf(sfOptions))
    }
    partitions.sortBy(_.id)
  }

  override def getSfOptions = sfOptions

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    queryUIOpt.foreach(_.updateStatus(TransformationStatus.PROCESSING_DATA))
    queryUIOpt.foreach(_.updateLastMicroBatchTime(System.currentTimeMillis))
    microBatchesCount += 1

    println(s"isNewData: $waitingForNewData")
    val printSoql = SoqlUtils.printSOQL(soqlWithDefaultFilters, isSelectAll)
    debug(s"2Salesforce query is: $printSoql")
    val data: List[InputPartition[InternalRow]] = toProcessPartitions.map { streamingPartition =>
      new StreamingSoapPartitionV2(
        soqlSer = SerializableSOQLQuery(SOQLParserHelper.createSOQLData(soqlWithDefaultFilters)),
        sfOptions = sfOptions,
        schema = resultSchema,
        sfSparkPartition = streamingPartition,
        checkpointLocationOpt = checkpointLocation,
        hdfsConf = serializableHadoopConf)
    }.toList
    data.asJava
  }

  override def setOffsetRange(start: Optional[Offset], `end`: Optional[Offset]): Unit = {
    debug(s"SetOffsetRange START: $start")
    debug(s"SetOffsetRange END: ${`end`}")
    if (`end`.isPresent) {
      debug("SetOffsetRange moving end to start")
      sfStartOffset = Some(SfOffset(`end`.get()))
    }
  }

  override def getStartOffset: Offset = {
    debug("GetStartOffset")
    throw new RuntimeException("Start offset is not defined")
  }

  override def getEndOffset: Offset = {
    initPartitions
    val processedPartitions = sfPartitionsRW
      .readExecutorPartitions
      .orElse(sfPartitionsRW.readDriverSfPartitions.map(_.partitions))
      .getOrElse(initPartitions)


    queryUIOpt.foreach(_.updateMaxOffset(maxOffset))

    Thread.sleep(500)
    info(s"Recreating partitions. Previous: \n${processedPartitions.map(e => s"ID: '${e.id}'. ${e.getWhereClause}. Finished: ${e.executorMetrics.isDone}").mkString("\n")}")
    if (!waitingForNewData) loadedRecordsSum = loadedRecordsSum + processedPartitions.map(_.executorMetrics.loadedRecords).sum

    toProcessPartitions = PartitionSplitter.recreateSfSparkPartitions(processedPartitions, sfOptions.sfLoadNumPartitions, offsetColName)
    sfPartitionsRW.writeDriverSfPartitions(SfStreamingPartitions(toProcessPartitions, maxOffset))
    sfPartitionsRW.deleteExecutorsPartitionsDir()


    queryUIOpt.foreach(_.updateMetrics(loadedRecordsSum, microBatchesCount, toProcessPartitions))
    Thread.sleep(500)
    if (toProcessPartitions.nonEmpty) {
      info(s"Recreated partitions. New: \n${toProcessPartitions.map(e => s"ID: '${e.id}'. ${e.getWhereClause}").mkString("\n")}")
    } else {
      queryUIOpt.foreach(_.updateStatus(TransformationStatus.WAITING_DATA))
      info(s"No new partition")
    }

    debug("GetEndOffset")


    val endOffset1 = if (toProcessPartitions.isEmpty) {
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
        lazy val latestOffset = sfSoapConnection.getLatestOffsetFromSF(offsetColName, sfTableName)
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
          info(s"New partitions are: \n${toProcessPartitions.map(e => s"ID: '${e.id}'. ${e.getWhereClause}").mkString("\n")}")
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

    println(s"EndOffset: $endOffsetMockCounter")
    endOffset1

  }

  override def deserializeOffset(json: String) = {
    debug("\n\n\n\n\n\n")
    debug(s"DeserializeOffset: $json")
    SfOffset(json)
  }

  override def commit(`end`: Offset): Unit = { }

  override def stop(): Unit = {
    debug("Stopping streaming")
  }

  private def getQueryWithMaxBound: String = {
    maxOffset.map { e =>
      val beforeSoql = SOQLParserHelper.createSOQLData(soqlWithDefaultFilters)
      val whereCause = s"$offsetColName > ${partitionColType.parseToSfValue(e)}"
      SoqlUtils.addWhereClause(beforeSoql, whereCause, setInParenthesis = true)
    }.getOrElse(SOQLParserHelper.createSOQLData(soqlWithDefaultFilters))
      .toSOQLText
  }

}
