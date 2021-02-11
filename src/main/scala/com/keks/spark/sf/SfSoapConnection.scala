package com.keks.spark.sf

import com.keks.spark.sf.exceptions.SalesforceConnectionException
import com.keks.spark.sf.implicits.RichTry
import com.keks.spark.sf.soap.SoapUtils.convertXmlObjectToXmlFieldsArray
import com.keks.spark.sf.soap.{SoapQueryExecutor, SoapQueryExecutorClassLoader}
import com.keks.spark.sf.util.{SfConfigUtils, SfUtils, UniqueQueryId}
import com.sforce.soap.partner.{PartnerConnection, QueryResult}
import com.sforce.ws.ConnectorConfig

import scala.util.Try


case class SfSoapConnection(soapConnection: PartnerConnection,
                            tableName: String,
                            queryExecutor: SoapQueryExecutor)(implicit queryId: UniqueQueryId) extends LogSupport {

  /* Salesforce column name with Spark Sql DataType */
  lazy val sfTableDataTypeMap = soapConnection
    .describeSObject(tableName)
    .getFields
    .filterNot(e => Seq("address", "location").contains(e.getType.name.toLowerCase))
    .map(SfUtils.parseSfFieldToDataType).toMap

  /**
    * Counting table by specific query.
    *
    * @param soql query
    * @param isQueryAll soap query all or not
    * @return number of records
    */
  def countTable(soql: String, isQueryAll: Boolean): Int = {
    val queryResult: QueryResult = if (isQueryAll) {
      soapConnection.queryAll(soql)
    } else {
      soapConnection.query(soql)
    }
    queryResult.getSize
  }

  /**
    * Finding latest offset in salesforce table
    * @param offsetColName column name
    * @return
    */
  def getLatestOffsetFromSF(offsetColName: String): Option[String] = {
    val soql = s"SELECT $offsetColName FROM $tableName ORDER BY $offsetColName DESC LIMIT 1"
    infoQ(s"Finding the last value in table: '$tableName' for column: '$offsetColName'")
    val value = getFirstRecordFirstColValue(soql)
    infoQ(s"Last value in table: '$tableName' for column: '$offsetColName' is: '$value'")
    value
  }

  def getFirstOffsetFromSF(offsetColName: String): Option[String] = {
    val soql = s"SELECT $offsetColName FROM $tableName ORDER BY $offsetColName LIMIT 1"
    infoQ(s"Finding the first value in table: '$tableName' for column: '$offsetColName'")
    val value = getFirstRecordFirstColValue(soql)
    infoQ(s"First value in table: '$tableName' for column: '$offsetColName' is: '$value'")
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

  def apply(sfOptions: SfOptions,
            sfTableName: String,
            connectionNameId: String)(implicit queryId: UniqueQueryId): SfSoapConnection = {
    debugQ(s"Connecting to Salesforce for connection name: '$connectionNameId'")
    val sfConnectionConfig: ConnectorConfig = SfConfigUtils.createSfConnectorConfig(sfOptions)
    val soapConnection = getSoapConnection(sfConnectionConfig, sfOptions.checkConnectionRetries, sfOptions.checkConnectionRetrySleepMin, connectionNameId)
    val queryExecutor =  SoapQueryExecutorClassLoader.loadClass(
      sfOptions.queryExecutorClassName,
      sfOptions,
      soapConnection,
      connectionNameId)
    new SfSoapConnection(soapConnection, sfTableName, queryExecutor)
  }

  private def getSoapConnection(conf: ConnectorConfig,
                                retries: Int,
                                sleepMillis: Long,
                                requesterSide: String)(implicit queryId: UniqueQueryId): PartnerConnection = {
    val leftTries = retries - 1
    if (leftTries == 0) {
      errorQ(s"Connection failed. Out of retires. Aborting")
      throw new SalesforceConnectionException(conf)
    } else {
      debugQ(s"Requester: '$requesterSide'. Connecting to Salesforce")
      Try(new PartnerConnection(conf)).onFailure { exp =>
        warnQ(s"Requester: '$requesterSide'. Cannot connect to Salesforce. Retrying: '$leftTries'.\n$exp")
        Thread.sleep(sleepMillis)
        getSoapConnection(conf, leftTries, sleepMillis, requesterSide)
      }
    }
  }

}
