package com.keks.spark.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SfOptions._
import com.keks.spark.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import org.apache.spark.sql.functions.col
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.xml._
import utils.{DataFrameEquality, MockedServer, TestBase}


class PrunedFiltersTest extends TestBase with MockedServer with DataFrameEquality {

  import spark.implicits._


  val soqlQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName WHERE isDeleted = false"

  def mockSf(): Unit = {
    val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
    val describeXmlResponse = SfDescribeXmlResponse(Seq(
      SfField(ID, "string"),
      SfField(SYSTEMMODSTAMP, "datetime"),
      SfField(TIME_FIELD, "time"),
      SfField(NAME, "string"))).toString
    val sfRecordsList = Seq(
      Seq(SfRecord(ID, Some("a")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("b")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("c")), SfRecord(TIME_FIELD, None), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("d")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, None)),
      Seq(SfRecord(ID, Some("e")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")))
      )
    val sfQueryResultXmlResponse = SfQueryResultXmlResponse("User", sfRecordsList).toString

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
              |WHERE (isDeleted = false AND (SystemModstamp > 2019-10-01T00:00:00.000Z)) AND
              |(SystemModstamp >= 2020-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
              |ORDER BY SystemModstamp""".stripMargin))
        .willReturn(aResponse().withBody(sfQueryResultXmlResponse))
      )
  }

  def getDefaultReader = spark
    .read
    .option(SF_INITIAL_OFFSET, "2020-01-01T00:00:00.000Z")
    .option(SF_END_OFFSET, "2020-01-02T00:00:00.000Z")
    .option(SF_USER_NAME, "")
    .option(SF_USER_PASSWORD, "")
    .option(SF_USE_HTTPS, value = false)
    .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
    .option(SF_API_VERSION, apiVersion)
    .option(SF_COMPRESSION, value = false)
    .option(SF_SHOW_TRACE, value = false)
    .option(SF_IS_QUERY_ALL, value = true)

  def tests(sfFormat: String): Unit = {
    it should "load data with pruned SYSTEMMODSTAMP" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()

      val result =
        getDefaultReader
          .format(sfFormat)
          .load(soqlQuery)
          .filter(col(SYSTEMMODSTAMP) > t"2019-10-01 00:00:00")
          .cache

      val expected = Seq(
        ("a", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("b", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("c", null, t"2020-10-23 13:39:45"),
        ("e", "13:39:45.000Z", t"2020-10-23 13:39:45")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

      assertDataFramesEqual(expected, result)
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}
