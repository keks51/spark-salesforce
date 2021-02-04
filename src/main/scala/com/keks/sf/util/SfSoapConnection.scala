package com.keks.sf.util

import com.keks.sf.exceptions.SalesforceConnectionException
import com.keks.sf.implicits.RichTry
import com.keks.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.sf.soap.{SoapQueryExecutor, SoapQueryExecutorClassLoader}
import com.keks.sf.{LogSupport, SfOptions}
import com.sforce.soap.partner.{Field, PartnerConnection, QueryResult}
import com.sforce.ws.ConnectorConfig

import scala.util.Try


case class SfSoapConnection(soapConnection: PartnerConnection, queryExecutor: SoapQueryExecutor) extends LogSupport {

  def describeObject(tableName: String): Array[Field] = {
    soapConnection
      .describeSObject(tableName)
      .getFields
      .filterNot(e => Seq("address", "location").contains(e.getType.name.toLowerCase))
  }

  def countTable(soql: String, isQueryAll: Boolean): Int = {
    val queryResult: QueryResult = if (isQueryAll) {
      soapConnection.queryAll(soql)
    } else {
      soapConnection.query(soql)
    }
    queryResult.getSize
  }

  def getLatestOffsetFromSF(offsetColName: String,
                            tableName: String): Option[String] = {
    val soql = s"SELECT $offsetColName FROM $tableName ORDER BY $offsetColName DESC LIMIT 1"
    info(s"Finding the last value in table: '$tableName' for column: '$offsetColName'")
    val value = getFirstRecordFirstColValue(soql)
    info(s"Last value in table: '$tableName' for column: '$offsetColName' is: '$value'")
    value
  }

  def getFirstOffsetFromSF(offsetColName: String,
                           tableName: String): Option[String] = {
    val soql = s"SELECT $offsetColName FROM $tableName ORDER BY $offsetColName LIMIT 1"
    info(s"Finding the first value in table: '$tableName' for column: '$offsetColName'")
    val value = getFirstRecordFirstColValue(soql)
    info(s"First value in table: '$tableName' for column: '$offsetColName' is: '$value'")
    value
  }

  private def queryData(soql: String): QueryResult = {
    queryExecutor.tryToQuery(
      soql = soql,
      batchCounter = 0,
      queryLocatorOpt = None,
      lastOffsetOpt = None)
  }

  private def getFirstRecordFirstColValue(soql: String): Option[String] = {
    for {
      headRecord <- queryData(soql).getRecords.headOption
      firstField <- convertXmlObjectToXmlFieldsArray(headRecord).headOption
      firstRecordValue <- Option(firstField.getValue)
    } yield {
      firstRecordValue.toString
    }
  }

}

object SfSoapConnection extends LogSupport {

  def apply(sfOptions: SfOptions, connectionNameId: String): SfSoapConnection = {
    info(s"Connecting to Salesforce for connection name: '$connectionNameId'")
    val sfConnectionConfig: ConnectorConfig = SfConfigUtils.createSfConnectorConfig(sfOptions)
    val soapConnection = getSoapConnection(sfConnectionConfig, sfOptions.checkConnectionRetries, sfOptions.checkConnectionRetrySleepMin, connectionNameId)
    val queryExecutor =  SoapQueryExecutorClassLoader.loadClass(
      sfOptions.queryExecutorClassName,
      sfOptions,
      soapConnection,
      connectionNameId)
    new SfSoapConnection(soapConnection, queryExecutor)
  }

  private def getSoapConnection(conf: ConnectorConfig,
                                retries: Int,
                                sleepMillis: Long,
                                requesterSide: String): PartnerConnection = {
    val leftTries = retries - 1
    if (leftTries == 0) {
      error(s"Requester: '$requesterSide'. Connection failed. Out of retires. Aborting")
      throw new SalesforceConnectionException(conf)
    } else {
      info(s"Requester: '$requesterSide'. Connecting to Salesforce")
      Try(new PartnerConnection(conf)).onFailure { exp =>
        warn(s"Requester: '$requesterSide'. Cannot connect to Salesforce. Retrying: '$leftTries'.\n$exp")
        Thread.sleep(sleepMillis)
        getSoapConnection(conf, leftTries, sleepMillis, requesterSide)
      }
    }
  }

}