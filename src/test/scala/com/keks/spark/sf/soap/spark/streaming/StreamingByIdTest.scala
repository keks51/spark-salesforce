package com.keks.spark.sf.soap.spark.streaming

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SALESFORCE_SOAP_V2
import com.keks.spark.sf.SfOptions._
import org.apache.spark.sql.streaming.OutputMode
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.xml._
import utils.{DataFrameEquality, MockedServer, TestBase, TmpDirectory}


class StreamingByIdTest extends TestBase with MockedServer with TmpDirectory with DataFrameEquality {

  import spark.implicits._

  "Spark with streaming v2 api" should "load all available data by Id" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
    withTempDir { dir =>
      val selectQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName"
      val whereQuery = s"WHERE isDeleted = false"
      val orderBySoql = s"ORDER BY $ID"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(TIME_FIELD, "time"),
        SfField(NAME, "string"))).toString

      val endOffset1 = Seq(
        Seq(SfRecord(ID, Some("h")))
        )
      val sfEndOffsetXmlResponse1 = SfQueryResultXmlResponse("User", endOffset1).toString
      val sfRecordsList1 = Seq(
        Seq(SfRecord(ID, Some("a")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("b")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse1 = SfQueryResultXmlResponse("User", sfRecordsList1, Some("locator_1")).toString

      val sfRecordsList2 = Seq(
        Seq(SfRecord(ID, Some("c")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-02-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("d")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-02-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse2 = SfQueryResultXmlResponse("User", sfRecordsList2, Some("locator_2"), queryMoreResponse = true).toString

      val sfRecordsList3 = Seq(
        Seq(SfRecord(ID, Some("e")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-03-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("f")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-03-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse3 = SfQueryResultXmlResponse("User", sfRecordsList3, Some("locator_3"), queryMoreResponse = true).toString

      val sfRecordsList4 = Seq(
        Seq(SfRecord(ID, Some("g")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-04-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("h")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-04-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse4 = SfQueryResultXmlResponse("User", sfRecordsList4, None, queryMoreResponse = true).toString

      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")
      stubSeqOfScenarios("test_scenario")(
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing("</m:describeSObject>"))
          .willReturn(aResponse().withBody(describeXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>SELECT Id FROM User ORDER BY Id DESC LIMIT 1</m:queryString>"))
          .willReturn(aResponse().withBody(sfEndOffsetXmlResponse1)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryString>$selectQuery WHERE (isDeleted = false) AND ($ID >= 'a' AND $ID &lt;= 'h') $orderBySoql</m:queryString>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse1)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_1</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse2)),
        post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_2</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse3)),
        post(requestUrl)
          .withRequestBody(containing(s"<m:queryLocator>locator_3</m:queryLocator>"))
          .willReturn(aResponse().withBody(sfQueryResultXmlResponse4))
        )

      val streamDF =
        spark
          .readStream
          .option(SF_OFFSET_COL, ID)
          .option(SF_STREAMING_QUERY_NAME, "User")
          .option(SF_INITIAL_OFFSET, "a")
          .option(SF_STREAMING_MAX_BATCHES, 2)
          .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = true)
          .option(SF_USER_NAME, "")
          .option(SF_USER_PASSWORD, "")
          .option(SF_USE_HTTPS, value = false)
          .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
          .option(SF_API_VERSION, apiVersion)
          .option(SF_COMPRESSION, value = false)
          .option(SF_SHOW_TRACE, value = false)
          .option(SF_IS_QUERY_ALL, value = true)
          .format(SALESFORCE_SOAP_V2)
          .load(selectQuery + " " + whereQuery)

      val saveDir = s"$dir/result"
      val query = streamDF.writeStream
        .outputMode(OutputMode.Append)
        .format("parquet")
        .option("path", saveDir)
        .option("checkpointLocation", s"$dir/$CHECKPOINT_DIR")
        .start()

      query.processAllAvailable()
      query.stop()

      val result = spark.read.parquet(saveDir)
      val expected = Seq(
        ("a", "00:00:00.000Z", t"2020-01-01 00:00:00"),
        ("b", "00:00:00.000Z", t"2020-01-02 00:00:00"),
        ("c", "00:00:00.000Z", t"2020-02-01 00:00:00"),
        ("d", "00:00:00.000Z", t"2020-02-02 00:00:00"),
        ("e", "00:00:00.000Z", t"2020-03-01 00:00:00"),
        ("f", "00:00:00.000Z", t"2020-03-02 00:00:00"),
        ("g", "00:00:00.000Z", t"2020-04-01 00:00:00"),
        ("h", "00:00:00.000Z", t"2020-04-02 00:00:00")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

      assertDataFramesEqual(expected, result)
    }
  }

}
