package com.keks.spark.sf.soap

import com.keks.spark.sf.enums.SoapDelivery.AT_LEAST_ONCE
import com.keks.spark.sf.implicits.RichTry
import com.keks.spark.sf.util.LetterTimeUnit.{H, M, S}
import com.keks.spark.sf.util.{DurationPrinter, SoqlUtils, UniqueQueryId, Utils}
import com.keks.spark.sf.{LogSupport, SfOptions}
import com.sforce.soap.partner.fault.InvalidQueryLocatorFault
import com.sforce.soap.partner.{PartnerConnection, QueryResult}
import com.sforce.ws.ConnectionException
import org.mule.tools.soql.SOQLParserHelper

import scala.util.Try


/**
  * Soap query executor implementation.
  *
  * @param sfOptions spark salesforce query options
  * @param soapConnection soap connection
  * @param executorName like 'Driver' or 'PartitionId: 1'
  */
class TrySoapQueryExecutor(override val sfOptions: SfOptions,
                           override val soapConnection: PartnerConnection,
                           override val executorName: String) extends SoapQueryExecutor(sfOptions,
                                                                                        soapConnection,
                                                                                        executorName) with LogSupport {

  /**
    * Executing query against Salesforce.
    * If invalid query locator issue then batch should be retried
    * and lowerBound offset condition should be >=. That will load already
    * loaded data and produce duplicates but guarantee data AT_LEAST_ONE semantic and
    * no data will be lost.
    * If offset column is not defined then loading is aborted.
    *
    * @param soql soql query
    * @param batchCounter batch number for logging
    * @param queryLocatorOpt soap batch query locator
    * @param lastOffsetOpt last offset if needed
    *  @return new batch
    */
  def tryToQuery(soql: String,
                 batchCounter: Int,
                 queryLocatorOpt: Option[String],
                 lastOffsetOpt: Option[Any])(implicit uniqueQueryId: UniqueQueryId): QueryResult = {
    Try {
      queryLocatorOpt
        .map { queryLocator => soapConnection.queryMore(queryLocator) }
        .getOrElse(if (sfOptions.isQueryAll) soapConnection.queryAll(soql) else soapConnection.query(soql))
    }.onFailure { exception =>
      val printSoql = SoqlUtils.printSOQL(SOQLParserHelper.createSOQLData(soql), sfOptions.isSelectAll)
      val sleep = Utils.getRandomDelay(sfOptions.checkConnectionRetrySleepMin, sfOptions.checkConnectionRetrySleepMax)
      warnQ(s"'$executorName'. Batch '$batchCounter' will be loaded again. Sleeping: ${DurationPrinter.print[H, M, S](sleep)} SOQL: '$printSoql'")
      Thread.sleep(sleep)
      (exception, queryLocatorOpt, lastOffsetOpt) match {
        case (_: ConnectionException, Some(_), _) if Option(exception.getMessage).exists(_.contains("Failed to send request")) =>
          warnQ(s"'$executorName'. Retrying Batch '$batchCounter'. Exception was: $exception")
          SoapUtils.checkConnection(soapConnection, sfOptions.checkConnectionRetries, sfOptions.checkConnectionRetrySleepMin, "PartitionId: '$partitionId'. Retrying Batch '$batchCounter'.")
          tryToQuery(soql, batchCounter, queryLocatorOpt, lastOffsetOpt)

        case (exception: InvalidQueryLocatorFault, _, Some(lastOffset)) =>
          warnQ(s"'$executorName'. Invalid Query Locator. Retrying to load data since lats offset: '$lastOffset'.  Soql: '$printSoql'")
          // TODO remove true
          val newSoql = SoqlUtils.replaceLowerOffsetBoundOrAddBound(soql, lastOffset.toString, AT_LEAST_ONCE).toSOQLText
          warnQ(s"'$executorName'. New Soql is: '$newSoql'")
          tryToQuery(newSoql, batchCounter, None, lastOffsetOpt)

        case (exception: InvalidQueryLocatorFault, None, None) =>
          warnQ(s"'$executorName'. First query failed with Invalid query locator. Retrying  Soql: '$printSoql'")
          tryToQuery(soql, batchCounter, None, lastOffsetOpt)

        case _ =>
          error(s"'$executorName'. Unknown exception:\n$exception")
          throw exception
      }
    }
  }

}

/**
  * Executing SOQL query against Salesforce table.
  * Handling exceptions and executing retries.
  * Custom implementation can be used by SoapQueryExecutorClassLoader.
  *
  * @param sfOptions spark salesforce query options
  * @param soapConnection soap connection
  * @param executorName like 'Driver' or 'PartitionId: 1'
  */
abstract class SoapQueryExecutor(val sfOptions: SfOptions,
                                 val soapConnection: PartnerConnection,
                                 val executorName: String) extends Serializable {

  /**
    * Trying to execute
    *
    * @param soql soql query
    * @param batchCounter batch number for logging
    * @param queryLocatorOpt soap batch query locator
    * @param lastOffsetOpt last offset if needed
    * @return new batch
    */
  def tryToQuery(soql: String,
                 batchCounter: Int,
                 queryLocatorOpt: Option[String],
                 lastOffsetOpt: Option[Any])
                (implicit uniqueQueryId: UniqueQueryId): QueryResult

}
