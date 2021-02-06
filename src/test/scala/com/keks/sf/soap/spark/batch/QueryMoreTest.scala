package com.keks.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.sf.SfOptions._
import com.keks.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.{DataFrameEquality, MockedServer, TestBase}
import xml._


class QueryMoreTest extends TestBase with MockedServer with DataFrameEquality {

  import spark.implicits._

  def tests(sfFormat: String): Unit = {

    it should "query more and return 4 rows" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      val soqlQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName WHERE isDeleted = false"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(TIME_FIELD, "time"),
        SfField(NAME, "string"))).toString
      val sfRecordsList1 = Seq(
        Seq(SfRecord(ID, Some("a")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
        Seq(SfRecord(ID, Some("b")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")))
        )
      val sfQueryResultXmlResponse1 = SfQueryResultXmlResponse("User", sfRecordsList1, Some("locator_1")).toString

      val sfRecordsList2 = Seq(
        Seq(SfRecord(ID, Some("c")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
        Seq(SfRecord(ID, Some("d")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")))
        )
      val sfQueryResultXmlResponse2 = SfQueryResultXmlResponse("User", sfRecordsList2, None, queryMoreResponse = true).toString


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
                |(SystemModstamp >= 2020-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
                |ORDER BY SystemModstamp""".stripMargin))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse1)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_1</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse2))
        )

      val result =
        spark
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
          .format(sfFormat)
          .load(soqlQuery)
          .cache

      val expected = Seq(
        ("a", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("b", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("c", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("d", "13:39:45.000Z", t"2020-10-23 13:39:45")
        ).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)
      assertDataFramesEqual(expected, result)
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)


}
