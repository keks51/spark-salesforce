package com.keks.spark.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SfOptions._
import com.keks.spark.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import utils.SalesforceColumns.{ID, SYSTEMMODSTAMP, TIME_FIELD}
import utils.xml._
import utils.{DataFrameEquality, MockedServer, TestBase}


class SelectAllQueryTest extends TestBase with MockedServer with DataFrameEquality {

  import spark.implicits._

  val soqlQuery = s"SELECT $ID,$SYSTEMMODSTAMP,$TIME_FIELD FROM $sfTableName WHERE isDeleted = false"
  val selectAllSoqlQuery = s"SELECT * FROM $sfTableName where isDeleted = false"

  def mockSf(expectedSoql: String): Unit = {

    val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
    val describeXmlResponse = SfDescribeXmlResponse(Seq(
      SfField(ID, "string"),
      SfField(SYSTEMMODSTAMP, "datetime"),
      SfField(TIME_FIELD, "time"))).toString
    val sfRecordsList = Seq(
      Seq(SfRecord(ID, Some("a")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")), SfRecord(TIME_FIELD, Some("13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("b")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")), SfRecord(TIME_FIELD, Some("13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("c")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")), SfRecord(TIME_FIELD, Some("13:39:45.000Z")))
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
        .withRequestBody(containing(expectedSoql))
        .willReturn(aResponse().withBody(sfQueryResultXmlResponse))
      )
  }

  val defaultReader = spark
    .read
    .option(SF_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
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
    it should "select all columns and load 5 records" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(qs"""SELECT Id,SystemModstamp,TimeField FROM User
                 |WHERE (isDeleted = false) AND
                 |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
                 |ORDER BY SystemModstamp""".stripMargin)

      val result =
        defaultReader
          .format(sfFormat)
          .load(selectAllSoqlQuery)
          .cache

      val expected = Seq(
        ("a", t"2020-10-23 13:39:45", "13:39:45.000Z"),
        ("b", t"2020-10-23 13:39:45", "13:39:45.000Z"),
        ("c", t"2020-10-23 13:39:45", "13:39:45.000Z")).toDF(ID, SYSTEMMODSTAMP, TIME_FIELD)

      assertDataFramesEqual(expected, result)
    }

    it should "select ID instead of all columns " in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(qs"""SELECT Id,SystemModstamp FROM User
                 |WHERE (isDeleted = false) AND
                 |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
                 |ORDER BY SystemModstamp""".stripMargin)

      val result =
        defaultReader
          .format(sfFormat)
          .load(selectAllSoqlQuery)
          .select(ID)
          .cache

      val expected = Seq(
        "a",
        "b",
        "c").toDF(ID)

      assertDataFramesEqual(expected, result)
    }

    it should "count with select all *" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(qs"""SELECT Id,SystemModstamp,TimeField FROM User
                 |WHERE (isDeleted = false) AND
                 |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
                 |ORDER BY SystemModstamp""".stripMargin)

      val result =
        defaultReader
          .format(sfFormat)
          .load(selectAllSoqlQuery)
          .count

      val exp = 3
      assert(result == exp)
    }

    it should "count with select all * and select ID" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(qs"""SELECT Id,SystemModstamp,TimeField FROM User
                 |WHERE (isDeleted = false) AND
                 |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
                 |ORDER BY SystemModstamp""".stripMargin)

      val result =
        defaultReader
          .format(sfFormat)
          .load(selectAllSoqlQuery)
          .select(ID)
          .count

      val exp = 3
      assert(result == exp)
    }

    it should "select all columns and load 5 records if only table is defined" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(qs"""SELECT Id,SystemModstamp,TimeField FROM User
                 |WHERE (SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
                 |ORDER BY SystemModstamp""".stripMargin)

      val result =
        defaultReader
          .format(sfFormat)
          .load(sfTableName)
          .cache

      val expected = Seq(
        ("a", t"2020-10-23 13:39:45", "13:39:45.000Z"),
        ("b", t"2020-10-23 13:39:45", "13:39:45.000Z"),
        ("c", t"2020-10-23 13:39:45", "13:39:45.000Z")).toDF(ID, SYSTEMMODSTAMP, TIME_FIELD)

      assertDataFramesEqual(expected, result)
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}
