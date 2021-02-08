package com.keks.spark.sf.soap.spark.streaming

import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.spark.sf.SALESFORCE_SOAP_V2
import com.keks.spark.sf.SfOptions._
import org.apache.spark.sql.streaming.OutputMode
import utils.SalesforceColumns.{ID, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.{DataFrameEquality, MockedServer, TestBase, TmpDirectory}
import xml._


class StreamingBy3PartitionsTest extends TestBase with MockedServer with DataFrameEquality with TmpDirectory {

  import spark.implicits._


  val SYSTEMMODSTAMP_VALUE = "2020-01-01T00:00:00.000Z"

  "Spark with streaming v2 api" should "load all available data by SystemModstamp" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
    withTempDir { dir =>
      val selectQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName"
      val whereQuery = s"WHERE isDeleted = false"
      val orderBySoql = s"ORDER BY $SYSTEMMODSTAMP"
      val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
      val describeXmlResponse = SfDescribeXmlResponse(Seq(
        SfField(ID, "string"),
        SfField(SYSTEMMODSTAMP, "datetime"),
        SfField(TIME_FIELD, "time"),
        SfField(NAME, "string"))).toString


      val bindUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion")
      val requestUrl: UrlPattern = urlEqualTo(s"$sfServicesEndPoint/$apiVersion/$sfId")


      val firstOffset1 = Seq(
        Seq(SfRecord(SYSTEMMODSTAMP, Some("2020-01-01T00:00:00.000Z")))
        )
      val sfFirstOffsetXmlResponse1 = SfQueryResultXmlResponse("User", firstOffset1).toString
      val endOffset1 = Seq(
        Seq(SfRecord(SYSTEMMODSTAMP, Some("2020-03-05T00:00:00.000Z")))
        )
      val sfEndOffsetXmlResponse1 = SfQueryResultXmlResponse("User", endOffset1).toString

      val sfRecordsList1 = Seq(
        Seq(SfRecord(ID, Some("a1")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("a2")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-01-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse1 = SfQueryResultXmlResponse("User", sfRecordsList1).toString

      val sfRecordsList2 = Seq(
        Seq(SfRecord(ID, Some("b1")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-02-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("b2")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-02-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse2 = SfQueryResultXmlResponse("User", sfRecordsList2).toString

      val sfRecordsList3 = Seq(
        Seq(SfRecord(ID, Some("c1")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-03-01T00:00:00.000Z"))),
        Seq(SfRecord(ID, Some("c2")), SfRecord(TIME_FIELD, Some("00:00:00.000Z")), SfRecord(SYSTEMMODSTAMP, Some("2020-03-02T00:00:00.000Z")))
        )
      val sfQueryResultXmlResponse3 = SfQueryResultXmlResponse("User", sfRecordsList3).toString


      stubFor(post(bindUrl)
                .willReturn(aResponse().withBody(bindingXmlResponse)))
      stubFor(post(requestUrl)
                .withRequestBody(containing("</m:describeSObject>")).willReturn(aResponse().withBody(describeXmlResponse)))
      stubFor(post(requestUrl)
        .withRequestBody(containing(s"<m:queryString>SELECT SystemModstamp FROM User ORDER BY SystemModstamp LIMIT 1</m:queryString>"))
        .willReturn(aResponse().withBody(sfFirstOffsetXmlResponse1)))
      stubFor(post(requestUrl)
        .withRequestBody(containing(s"<m:queryString>SELECT SystemModstamp FROM User ORDER BY SystemModstamp DESC LIMIT 1</m:queryString>"))
        .willReturn(aResponse().withBody(sfEndOffsetXmlResponse1)))
      stubFor(post(bindUrl).willReturn(aResponse().withBody(bindingXmlResponse)))
      stubFor(post(requestUrl)
                .withRequestBody(containing(
                  s"<m:queryString>$selectQuery WHERE (isDeleted = false) AND (SystemModstamp >= 2020-01-01T00:00:00.000Z AND SystemModstamp &lt; 2020-01-22T08:00:00.000Z) $orderBySoql</m:queryString>"))
                .willReturn(aResponse().withBody(sfQueryResultXmlResponse1)))
      stubFor(post(requestUrl)
                .withRequestBody(containing(
                  s"<m:queryString>$selectQuery WHERE (isDeleted = false) AND (SystemModstamp >= 2020-01-22T08:00:00.000Z AND SystemModstamp &lt; 2020-02-12T16:00:00.000Z) $orderBySoql</m:queryString>"))
                .willReturn(aResponse().withBody(sfQueryResultXmlResponse2)))
      stubFor(post(requestUrl)
                .withRequestBody(containing(
                  s"<m:queryString>$selectQuery WHERE (isDeleted = false) AND (SystemModstamp >= 2020-02-12T16:00:00.000Z AND SystemModstamp &lt;= 2020-03-05T00:00:00.000Z) $orderBySoql</m:queryString>"))
                .willReturn(aResponse().withBody(sfQueryResultXmlResponse3)))

      val streamDF =
        spark
          .readStream
          .option(SF_OFFSET_COL, SYSTEMMODSTAMP)
          .option(SF_STREAMING_QUERY_NAME, "User")
          .option(SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING, 60000)
          .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = true)
          .option(SF_LOAD_NUM_PARTITIONS, 3)
          .option(SF_STREAMING_MAX_BATCHES, 2)
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
      result.show(false)
      val expected = Seq(
        ("a1", "00:00:00.000Z", t"2020-01-01 00:00:00"),
        ("a2", "00:00:00.000Z", t"2020-01-02 00:00:00"),
        ("b1", "00:00:00.000Z", t"2020-02-01 00:00:00"),
        ("b2", "00:00:00.000Z", t"2020-02-02 00:00:00"),
        ("c1", "00:00:00.000Z", t"2020-03-01 00:00:00"),
        ("c2", "00:00:00.000Z", t"2020-03-02 00:00:00")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

      assertDataFramesEqual(expected, result)
    }
  }


}
