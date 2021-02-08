package com.keks.spark.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SfOptions._
import com.keks.spark.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.{MockedServer, TestBase}
import xml._


class EmptyResultTest extends TestBase with MockedServer {

  val soqlQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName WHERE isDeleted = false"

  def mockSf(): Unit = {
    val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
    val describeXmlResponse = SfDescribeXmlResponse(Seq(
      SfField(ID, "string"),
      SfField(SYSTEMMODSTAMP, "datetime"),
      SfField(TIME_FIELD, "time"),
      SfField(NAME, "string"))).toString

    val sfQueryResultXmlResponse = SfQueryResultXmlResponse("User", emptyRecordsList).toString

    val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
    val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")
    stubSeqOfScenarios("test_scenario")(
      post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
      post(requestUrl)
        .withRequestBody(containing("</m:describeSObject>"))
        .willReturn(aResponse().withBody(describeXmlResponse)),
      post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
      post(requestUrl)
        .withRequestBody(containing(
          qs"""SELECT Id,TimeField,SystemModstamp FROM User
              |WHERE (isDeleted = false) AND
              |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
              |ORDER BY SystemModstamp""".stripMargin))
        .willReturn(aResponse().withBody(sfQueryResultXmlResponse))
      )
  }

  def  getDefaultReader = spark
    .read
    .option(SF_USER_NAME, "")
    .option(SF_USER_PASSWORD, "")
    .option(SF_USE_HTTPS, value = false)
    .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
    .option(SF_API_VERSION, apiVersion)
    .option(SF_COMPRESSION, value = false)
    .option(SF_SHOW_TRACE, value = false)
    .option(SF_IS_QUERY_ALL, value = true)

  def tests(sfFormat: String): Unit = {
    it should "return empty result" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()

      val result =
        getDefaultReader
          .format(sfFormat)
          .option(SF_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
          .option(SF_END_OFFSET, "2020-01-01T00:00:00.000Z")
          .load(soqlQuery)
          .cache
      assert(result.rdd.isEmpty)
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}
