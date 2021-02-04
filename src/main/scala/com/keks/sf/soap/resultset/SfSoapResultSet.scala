package com.keks.sf.soap.resultset

import com.keks.sf.soap.SfSparkPartition
import com.keks.sf.util.SoqlUtils
import com.keks.sf.{SfOptions, SfResultSet}
import com.sforce.soap.partner.QueryResult
import com.sforce.soap.partner.sobject.SObject
import org.mule.tools.soql.SOQLParserHelper

import scala.collection.mutable.ArrayBuffer
import scala.util.Try


abstract class SfSoapResultSet(sfOptions: SfOptions,
                               firstQueryResult: QueryResult,
                               soql: String,
                               sfPartition: SfSparkPartition,
                               partitionId: Int) extends SfResultSet {

  protected val printSoql: String = SoqlUtils.printSOQL(SOQLParserHelper.createSOQLData(soql), sfOptions.isSelectAll)

  protected val globalNumberOfRecords: Int = firstQueryResult.getSize
  info(s"PartitionId: '$partitionId'. For query '$printSoql' \n   result size is '$globalNumberOfRecords' records.")

  protected var processedRecordCount = 0
  protected var currentQueryResult: QueryResult = firstQueryResult
  protected var currentBatch: Array[SObject] = firstQueryResult.getRecords
  protected var recordsInBatch: Int = firstQueryResult.getRecords.length
  val maxBatchNumber: Int = Try(Math.ceil(globalNumberOfRecords.asInstanceOf[Double] / recordsInBatch).toInt).toOption.getOrElse(0)
  info(s"PartitionId: '$partitionId'. Estimated batches number is '$maxBatchNumber'")
  protected var batchCursor: Int = -1
  protected var batchCounter = 1

  protected var sparkBatchProcessingStartTime: Long = getCurrentTime
  protected var sparkBatchProcessingTimes: ArrayBuffer[Long] = ArrayBuffer.empty[Long]
  protected val partitionProcessingStartTime: Long = getCurrentTime
  sfPartition.setRecordsToLoad(globalNumberOfRecords)

  protected def isBatchEmpty(batchCursor: Int): Boolean = {
    recordsInBatch - 1 == batchCursor
  }

}
