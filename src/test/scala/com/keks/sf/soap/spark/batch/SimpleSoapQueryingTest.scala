package com.keks.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.sf.SfOptions._
import com.keks.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrameReader, Dataset}
import utils.SalesforceColumns._
import utils.{DataFrameEquality, MockedServer, TestBase, TmpDirectory}
import xml._

import java.sql.Timestamp


class SimpleSoapQueryingTest extends TestBase with MockedServer with DataFrameEquality with TmpDirectory {

  import spark.implicits._


  val selectQuery = s"SELECT $ID,$TIME_FIELD,$SYSTEMMODSTAMP FROM $sfTableName"

  def mockSf(expectedSoql: String): Unit = {
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
        .withRequestBody(containing(s"<m:queryString>$expectedSoql</m:queryString>"))
        .willReturn(aResponse().withBody(sfQueryResultXmlResponse))
      )
  }

  def getDefaultReader: DataFrameReader = spark
    .read
    .option(SF_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
    .option(SF_END_OFFSET, "2020-01-01T00:00:00.000Z")
    .option(SF_USER_NAME, "")
    .option(SF_USER_PASSWORD, "")
    .option(SF_USE_HTTPS, value = false)
    .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
    .option(SF_API_VERSION, apiVersion)
    .option(SF_COMPRESSION, value = false)
    .option(SF_SHOW_TRACE, value = false)
    .option(SF_IS_QUERY_ALL, value = true)

  def tests(sfFormat: String): Unit = {
    it should "load 5 records" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(
        s"""SELECT Id,TimeField,SystemModstamp FROM User
           |WHERE (isDeleted = false) AND
           |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
           |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

      val result =
        getDefaultReader
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .cache

      val expected = Seq(
        ("a", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("b", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("c", null, t"2020-10-23 13:39:45"),
        ("d", "13:39:45.000Z", null),
        ("e", "13:39:45.000Z", t"2020-10-23 13:39:45")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

      assertDataFramesEqual(expected, result)
    }

    it should "count 5 records" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(
        s"""SELECT Id,TimeField,SystemModstamp FROM User
           |WHERE (isDeleted = false) AND
           |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
           |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

      val result =
        getDefaultReader
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .count

      val exp = 5L
      assert(exp == result)
    }

    it should "load 5 records and map to SfData class" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(s"$selectQuery WHERE (isDeleted = false) AND (SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z) ORDER BY SystemModstamp")
      printf("here2")
      val result: Dataset[SfData] =
        getDefaultReader
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .as[SfData]
          .map(identity)
          .cache
      val expected: Dataset[SfData] = Seq(
        SfData(t"2020-10-23 13:39:45", "a", "13:39:45.000Z"),
        SfData(t"2020-10-23 13:39:45", "b", "13:39:45.000Z"),
        SfData(t"2020-10-23 13:39:45", "c", null),
        SfData(null, "d", "13:39:45.000Z"),
        SfData(t"2020-10-23 13:39:45", "e", "13:39:45.000Z")).toDS

      assertDatasetsEqual(expected, result)

    }

    it should "load 5 records and select ID" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(
        s"""SELECT Id,SystemModstamp FROM User
           |WHERE (isDeleted = false) AND
           |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
           |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

      val result =
        getDefaultReader
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .select(ID)
          .cache

      val expected = Seq(
        "a",
        "b",
        "c",
        "d",
        "e").toDF(ID)


      assertDataFramesEqual(expected, result)
    }

    it should "load 5 records with Where" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(
        s"""SELECT Id,TimeField,SystemModstamp FROM User
           |WHERE ((SystemModstamp > 2020-10-10T01:05:24.000Z)) AND
           |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
           |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

      val result =
        getDefaultReader
          .format(sfFormat)
          .load(s"$selectQuery")
          .filter(col(SYSTEMMODSTAMP) > t"2020-10-10 01:05:24")
          .cache

      val expected = Seq(
        ("a", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("b", "13:39:45.000Z", t"2020-10-23 13:39:45"),
        ("c", null, t"2020-10-23 13:39:45"),
        ("e", "13:39:45.000Z", t"2020-10-23 13:39:45")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)


      assertDataFramesEqual(expected, result)
    }

    it should "load 5 records and group by" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(
        s"""SELECT Id,SystemModstamp FROM User
           |WHERE (isDeleted = false) AND
           |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
           |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

      val result =
        getDefaultReader
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .groupBy(ID, SYSTEMMODSTAMP)
          .count()
          .cache

      val expected = Seq(
        ("a", t"2020-10-23 13:39:45", 1L),
        ("b", t"2020-10-23 13:39:45", 1L),
        ("c", t"2020-10-23 13:39:45", 1L),
        ("d", null, 1L),
        ("e", t"2020-10-23 13:39:45", 1L)).toDF(ID, SYSTEMMODSTAMP, "count")

      assertDataFramesEqual(expected, result)
    }

    it should "save data in parquet" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      withTempDir { dir =>
        mockSf(
          s"""SELECT Id,TimeField,SystemModstamp FROM User
             |WHERE (isDeleted = false) AND
             |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
             |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

        spark
          .read
          .option(SF_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
          .option(SF_END_OFFSET, "2020-01-01T00:00:00.000Z")
          .option(SF_USER_NAME, "")
          .option(SF_USER_PASSWORD, "")
          .option(SF_USE_HTTPS, value = false)
          .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
          .option(SF_API_VERSION, apiVersion)
          .option(SF_COMPRESSION, value = false)
          .option(SF_SHOW_TRACE, value = false)
          .option(SF_IS_QUERY_ALL, value = true)
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .write
          .parquet(s"$dir/$SF_TABLE_NAME")

        val exp = Seq(
          ("a", "13:39:45.000Z", t"2020-10-23 13:39:45"),
          ("b", "13:39:45.000Z", t"2020-10-23 13:39:45"),
          ("c", null, t"2020-10-23 13:39:45"),
          ("d", "13:39:45.000Z", null),
          ("e", "13:39:45.000Z", t"2020-10-23 13:39:45")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

        val res = spark.read.parquet(s"$dir/$SF_TABLE_NAME")

        assertDataFramesEqual(exp, res)
      }
    }

    it should "save data in csv" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      withTempDir { dir =>
        mockSf(
          s"""SELECT Id,TimeField,SystemModstamp FROM User
             |WHERE (isDeleted = false) AND
             |(SystemModstamp >= 2000-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-01T00:00:00.000Z)
             |ORDER BY SystemModstamp""".stripMargin.lines.map(_.trim).mkString(" "))

        spark
          .read
          .option(SF_INITIAL_OFFSET, "2000-01-01T00:00:00.000Z")
          .option(SF_END_OFFSET, "2020-01-01T00:00:00.000Z")
          .option(SF_USER_NAME, "")
          .option(SF_TABLE_NAME, sfTableName)
          .option(SF_USER_PASSWORD, "")
          .option(SF_USE_HTTPS, value = false)
          .option(SF_AUTH_END_POINT, s"$wireMockServerHost:$wireMockServerPort")
          .option(SF_API_VERSION, apiVersion)
          .option(SF_COMPRESSION, value = false)
          .option(SF_SHOW_TRACE, value = false)
          .option(SF_IS_QUERY_ALL, value = true)
          .format(sfFormat)
          .load(s"$selectQuery  WHERE isDeleted = false")
          .write
          .option("header", "true")
          .csv(s"$dir/$SF_TABLE_NAME")

        val exp = Seq(
          ("a", "13:39:45.000Z", "2020-10-23T13:39:45.000+03:00"),
          ("b", "13:39:45.000Z", "2020-10-23T13:39:45.000+03:00"),
          ("c", null, "2020-10-23T13:39:45.000+03:00"),
          ("d", "13:39:45.000Z", null),
          ("e", "13:39:45.000Z", "2020-10-23T13:39:45.000+03:00")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)

        val res = spark.read.option("header", "true").csv(s"$dir/$SF_TABLE_NAME")

        assertDataFramesEqual(exp, res)
      }
    }
  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

  //  "fdfdf" should "load 5 records" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
  //    mockSf()
  //
  //    val result =
  //      getDefaultReader
  //        .format(SALESFORCE_SOAP_V1)
  //        .load(s"$selectQuery  WHERE isDeleted = false")
  //        .cache
  //
  //    val expected = Seq(
  //      ("a", "13:39:45.000Z", t"2020-10-23 13:39:45"),
  //      ("b", "13:39:45.000Z", t"2020-10-23 13:39:45"),
  //      ("c", null, t"2020-10-23 13:39:45"),
  //      ("d", "13:39:45.000Z", null),
  //      ("e", "13:39:45.000Z", t"2020-10-23 13:39:45")).toDF(ID, TIME_FIELD, SYSTEMMODSTAMP)
  //
  //    assertDataFramesEqual(expected, result)
  //  }

}

case class SfData(systemModstamp: Timestamp,
                  id: String,
                  timeField: String)
