package com.keks.spark.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SfOptions._
import com.keks.spark.sf.soap.DEFAULT_SOAP_QUERY_EXECUTOR_CLASS
import com.keks.spark.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.xml._
import utils.{DataFrameEquality, MockedServer, TestBase}


class LastOffsetIdTest extends TestBase with MockedServer with DataFrameEquality {

  import spark.implicits._


  val TIME_VALUE = "13:39:45.000Z"
  val SYSTEMMODSTAMP_VALUE = "2020-01-01T00:00:00.000Z"

  def tests(sfFormat: String): Unit = {
    it should "load data with Id offset" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      val soqlQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName"


      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(TIME_FIELD, "time"),
        SfField(NAME, "string"))).toString


      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")


      val sfRecordsList1 = Seq(
        Seq(SfRecord(ID, Some("a")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("b")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-01T00:00:01.000Z"))),
        Seq(SfRecord(ID, Some("c")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-01T00:00:02.000Z")))
        )
      val sfQueryResultXmlResponse1 = SfQueryResultXmlResponse(sfTableName, sfRecordsList1, Some(s"locator_1")).toString

      val sfRecordsList2: Seq[Seq[SfRecord]] = Seq(
        Seq(SfRecord(ID, Some("d")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-02T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("e")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-02T00:00:01.000Z"))),
        Seq(SfRecord(ID, Some("f")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-02T00:00:02.000Z")))
        )
      val sfQueryResultXmlResponse2 = SfQueryResultXmlResponse(sfTableName, sfRecordsList2, Some(s"locator_2")).toString


      val sfQueryResultXmlResponse3 = SfQueryResultXmlResponse(sfTableName, emptyRecordsList, None, queryMoreResponse = true).toString

      stubSeqOfScenarios("test_scenario")(
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing("</m:describeSObject>"))
          .willReturn(aResponse().withBody(describeXmlResponse)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(
            qs"""SELECT Id,TimeField,SystemModstamp FROM User
                |WHERE (Id >= 'a' AND Id &lt;= 'f')
                |ORDER BY Id""".stripMargin))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse1)),
        post(requestUrl)
          .withRequestBody(containing(ql"locator_1"))
          .willReturn(aResponse().withStatus(401).withBody(InvalidQueryLocatorResponse().toString)),
        post(requestUrl)
          .withRequestBody(containing(
            qs"""SELECT Id,TimeField,SystemModstamp FROM User
                |WHERE (Id >= 'c' AND Id &lt;= 'f')
                |ORDER BY Id""".stripMargin))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse2)),
        post(requestUrl)
          .withRequestBody(containing(ql"locator_2"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse3))

        )

      val result =
        spark
          .read
          .option(SF_OFFSET_COL, ID)
          .option(SF_INITIAL_OFFSET, "'a'")
          .option(SF_END_OFFSET, "f")
          .option(SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MIN, 100)
          .option(SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MAX, 100)
          .option(SF_USER_NAME, "")
          .option(SOAP_QUERY_EXECUTOR_CLASS_NAME, DEFAULT_SOAP_QUERY_EXECUTOR_CLASS)
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
        ("a", "13:39:45.000Z", t"2020-01-01 00:00:00"),
        ("b", "13:39:45.000Z", t"2020-01-01 00:00:01"),
        ("c", "13:39:45.000Z", t"2020-01-01 00:00:02"),
        ("d", "13:39:45.000Z", t"2020-01-02 00:00:00"),
        ("e", "13:39:45.000Z", t"2020-01-02 00:00:01"),
        ("f", "13:39:45.000Z", t"2020-01-02 00:00:02")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

      assertDataFramesEqual(expected, result)
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}
