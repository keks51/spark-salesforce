package com.keks.sf.soap

import com.keks.sf.LogSupport
import com.keks.sf.exceptions.SalesforceConnectionException
import com.keks.sf.implicits.RichTry
import com.sforce.soap.partner.{GetServerTimestampResult, PartnerConnection}
import com.sforce.ws.bind.XmlObject

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.Try


object SoapUtils extends LogSupport {

  // first 2 cols are hardcoded table name and id
  def convertXmlObjectToXmlFieldsArray(xmlRecord: XmlObject): Array[XmlObject] =
    xmlRecord
      .getChildren
      .asScala
      .drop(2)
      .toArray



  def checkConnection(soapConnection: PartnerConnection,
                      retries: Int,
                      sleepMillis: Long,
                      requesterSide: String): GetServerTimestampResult = {
    val leftTries = retries - 1
    if (leftTries == 0) {
      error(s"Requester: '$requesterSide'. Connection failed. Out of retires. Aborting")
      throw new SalesforceConnectionException(soapConnection.getConfig)
    } else {
      info(s"Requester: '$requesterSide'. Checking connection to Salesforce")
      Try(soapConnection.getServerTimestamp).onFailure { exp =>
        warn(s"Requester: '$requesterSide'. Cannot connect to Salesforce. Retrying: '$leftTries'.\n$exp")
        Thread.sleep(sleepMillis)
        checkConnection(soapConnection, leftTries, sleepMillis, requesterSide)
      }
    }
  }

}
