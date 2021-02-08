package com.keks.spark.sf.soap.resultset

import com.keks.spark.sf.soap.SfSparkPartition
import com.keks.spark.sf.util.SoqlUtils
import com.keks.spark.sf.{SfOptions, SfResultSet}
import com.sforce.soap.partner.QueryResult
import com.sforce.soap.partner.sobject.SObject
import org.mule.tools.soql.SOQLParserHelper

import scala.collection.mutable.ArrayBuffer
import scala.util.Try


/**
  * Like Jdbc result set for getting salesforce data.
  * Takes first soap query result and continue loading
  *
  * @param sfOptions spark query options
  * @param firstQueryResult first soap query result
  * @param soql query soql
  * @param sfPartition executor partition
  */
abstract class SfSoapResultSet(sfOptions: SfOptions,
                               firstQueryResult: QueryResult,
                               soql: String,
                               sfPartition: SfSparkPartition) extends SfResultSet {
  protected val partitionId: Int = sfPartition.id
  protected val prettySoql: String = SoqlUtils.printSOQL(SOQLParserHelper.createSOQLData(soql), sfOptions.isSelectAll)

  /* From firstQueryResult we can get the number of records that will be returned for this soql */
  protected val globalNumberOfRecords: Int = firstQueryResult.getSize
  info(s"PartitionId: '$partitionId'. For query '$prettySoql' \n   result size is '$globalNumberOfRecords' records.")

  /* number of processed records by spark  */
  protected var processedRecordCount = 0
  protected var currentQueryResult: QueryResult = firstQueryResult
  protected var currentBatch: Array[SObject] = firstQueryResult.getRecords

  /* Each batch can contains different number of records */
  protected var recordsInBatch: Int = firstQueryResult.getRecords.length
  /* Approximate number of coming batches */
  val maxBatchNumber: Int = Try(Math.ceil(globalNumberOfRecords.asInstanceOf[Double] / recordsInBatch).toInt).toOption.getOrElse(0)
  info(s"PartitionId: '$partitionId'. Estimated batches number is '$maxBatchNumber'")
  /* keep position of processed records */
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
