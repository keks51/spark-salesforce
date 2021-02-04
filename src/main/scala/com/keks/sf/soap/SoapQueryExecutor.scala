package com.keks.sf.soap

import com.keks.sf.enums.SoapDelivery
import com.keks.sf.implicits.RichTry
import com.keks.sf.util.LetterTimeUnit.{H, M, S}
import com.keks.sf.util.{DurationPrinter, SoqlUtils, Utils}
import com.keks.sf.{LogSupport, SfOptions}
import com.sforce.soap.partner.fault.InvalidQueryLocatorFault
import com.sforce.soap.partner.{PartnerConnection, QueryResult}
import com.sforce.ws.ConnectionException
import org.mule.tools.soql.SOQLParserHelper

import scala.util.Try


class TrySoapQueryExecutor(override val sfOptions: SfOptions,
                           override val soapConnection: PartnerConnection,
                           override val executorName: String) extends SoapQueryExecutor(sfOptions,
                                                                                        soapConnection,
                                                                                        executorName) with LogSupport {

  def tryToQuery(soql: String,
                 batchCounter: Int,
                 queryLocatorOpt: Option[String],
                 lastOffsetOpt: Option[Any]): QueryResult = {
    Try {
      queryLocatorOpt
        .map { queryLocator => soapConnection.queryMore(queryLocator) }
        .getOrElse(if (sfOptions.isQueryAll) soapConnection.queryAll(soql) else soapConnection.query(soql))
    }.onFailure { exception =>
      val printSoql = SoqlUtils.printSOQL(SOQLParserHelper.createSOQLData(soql), sfOptions.isSelectAll)
      val sleep = Utils.getRandomDelay(sfOptions.checkConnectionRetrySleepMin, sfOptions.checkConnectionRetrySleepMax)
      warn(s"'$executorName'. Batch '$batchCounter' will be loaded again. Sleeping: ${DurationPrinter.print[H, M, S](sleep)} SOQL: '$printSoql'")
      Thread.sleep(sleep)
      (exception, queryLocatorOpt, lastOffsetOpt) match {
        case (_: ConnectionException, Some(_), _) if Option(exception.getMessage).exists(_.contains("Failed to send request")) =>
          warn(s"'$executorName'. Retrying Batch '$batchCounter'. Exception was: $exception")
          SoapUtils.checkConnection(soapConnection, sfOptions.checkConnectionRetries, sfOptions.checkConnectionRetrySleepMin, "PartitionId: '$partitionId'. Retrying Batch '$batchCounter'.")
          tryToQuery(soql, batchCounter, queryLocatorOpt, lastOffsetOpt)

        case (exception: InvalidQueryLocatorFault, _, Some(lastOffset)) =>
          warn(s"'$executorName'. Invalid Query Locator. Retrying to load data since lats offset: '$lastOffset'.  Soql: '$printSoql'")
          // TODO remove true
          val newSoql = SoqlUtils.replaceLowerOffsetBoundOrAddBound(soql, lastOffset.toString, SoapDelivery.AT_LEAST_ONCE).toSOQLText
          warn(s"'$executorName'. New Soql is: '$newSoql'")
          tryToQuery(newSoql, batchCounter, None, lastOffsetOpt)

        case (exception: InvalidQueryLocatorFault, None, None) =>
          warn(s"'$executorName'. First query failed with Invalid query locator. Retrying  Soql: '$printSoql'")
          tryToQuery(soql, batchCounter, None, lastOffsetOpt)

        case _ =>
          error(s"'$executorName'. Unknown exception:\n$exception")
          throw exception
      }
    }
  }

}

abstract class SoapQueryExecutor(val sfOptions: SfOptions,
                                 val soapConnection: PartnerConnection,
                                 val executorName: String) extends Serializable {

  def tryToQuery(soql: String,
                 batchCounter: Int,
                 queryLocatorOpt: Option[String],
                 lastOffsetOpt: Option[Any]): QueryResult

}
