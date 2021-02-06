package com.keks.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.sf.SfOptions._
import com.keks.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import utils.SalesforceColumns._
import utils.{DataFrameEquality, MockedServer, TestBase}
import xml._


class PartitionedByIntegerTest extends TestBase with MockedServer with DataFrameEquality {

  import spark.implicits._


  def tests(sfFormat: String): Unit = {
    it should "load data partitioned by Integer in 3 partitions" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      val soqlQuery = s"SELECT $ID,$AGE,$SYSTEMMODSTAMP FROM $sfTableName"
      val whereStr = "WHERE isDeleted = false"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(AGE, "int"),
        SfField(NAME, "string"))).toString
      val sfRecordsList1 = Seq(
        Seq(SfRecord(ID, Some("a")), SfRecord(AGE, Some("1")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
        Seq(SfRecord(ID, Some("b")), SfRecord(AGE, Some("1")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")))
        )

      val sfRecordsList3 = Seq(
        Seq(SfRecord(ID, Some("e")), SfRecord(AGE, Some("1")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z"))),
        Seq(SfRecord(ID, Some("f")), SfRecord(AGE, Some("1")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")))
        )
      val sfQueryResultXmlResponse1 = SfQueryResultXmlResponse("User", sfRecordsList1).toString
      val sfQueryResultXmlResponse2 = SfQueryResultXmlResponse("User", emptyRecordsList).toString
      val sfQueryResultXmlResponse3 = SfQueryResultXmlResponse("User", sfRecordsList3).toString


      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")
      stubFor(post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)))
      stubFor(post(requestUrl).withRequestBody(containing("</m:describeSObject>")).willReturn(aResponse().withBody(describeXmlResponse)))
      stubFor(post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)))
      stubFor(post(requestUrl)
                .withRequestBody(containing(
                  qs"""SELECT Id,Age,SystemModstamp FROM User
                      |WHERE (isDeleted = false) AND
                      |(Age >= 1 AND Age &lt; 5)
                      |ORDER BY Age""".stripMargin))
                .willReturn(aResponse().withBody(sfQueryResultXmlResponse1)))
      stubFor(post(requestUrl)
                .withRequestBody(containing(
                  qs"""SELECT Id,Age,SystemModstamp FROM User
                      |WHERE (isDeleted = false) AND
                      |(Age >= 5 AND Age &lt; 9)
                      |ORDER BY Age""".stripMargin))
                .willReturn(aResponse().withBody(sfQueryResultXmlResponse2)))
      stubFor(post(requestUrl)
                .withRequestBody(containing(
                  qs"""SELECT Id,Age,SystemModstamp FROM User
                      |WHERE (isDeleted = false) AND
                      |(Age >= 9 AND Age &lt;= 15)
                      |ORDER BY Age""".stripMargin))
                .willReturn(aResponse().withBody(sfQueryResultXmlResponse3)))

      val result =
        spark
          .read
          .option(SF_INITIAL_OFFSET, "1")
          .option(SF_END_OFFSET, "15")
          .option(SF_USER_NAME, "")
          .option(SF_OFFSET_COL, AGE)
          .option(SF_USER_PASSWORD, "")
          .option(SF_USE_HTTPS, value = false)
          .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
          .option(SF_API_VERSION, apiVersion)
          .option(SF_COMPRESSION, value = false)
          .option(SF_SHOW_TRACE, value = false)
          .option(SF_IS_QUERY_ALL, value = true)
          .option(SF_LOAD_NUM_PARTITIONS, 3)
          .format(sfFormat)
          .load(soqlQuery + " " + whereStr)
          .cache()


      val expected = Seq(
        ("a", 1, t"2020-10-23 13:39:45"),
        ("b", 1, t"2020-10-23 13:39:45"),
        ("e", 1, t"2020-10-23 13:39:45"),
        ("f", 1, t"2020-10-23 13:39:45")
        ).toDF(ID, AGE, SYSTEMMODSTAMP)

      assertDataFramesEqual(expected, result)
    }

  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}
