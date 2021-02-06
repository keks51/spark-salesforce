package com.keks.sf.soap.spark.batch

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, containing, post, urlEqualTo}
import com.github.tomakehurst.wiremock.matching.UrlPattern
import com.keks.sf.SfOptions._
import com.keks.sf.exceptions.UnsupportedSchemaWithSelectException
import com.keks.sf.{SALESFORCE_SOAP_V1, SALESFORCE_SOAP_V2}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import utils.SalesforceColumns.{AGE, ID, LASTMODIFIEDDATE, NAME, SYSTEMMODSTAMP, TIME_FIELD}
import utils.{DataFrameEquality, MockedServer, TestBase, TmpDirectory}
import xml._

import java.sql.Timestamp


class SchemaAndSelectedTest extends TestBase with MockedServer with DataFrameEquality with TmpDirectory {

  import spark.implicits._


  val soqlQuery = s"SELECT $SYSTEMMODSTAMP,$ID FROM $sfTableName WHERE isDeleted = false"
  val selectAllSoql = s"SELECT * FROM $sfTableName WHERE isDeleted = false"

  val expectedDefaultSoql =
    qs"""SELECT SystemModstamp,Id FROM User
        |WHERE (isDeleted = false) AND
        |(SystemModstamp >= 2020-01-01T00:00:00.000Z AND SystemModstamp &lt;= 2020-01-02T00:00:00.000Z)
        |ORDER BY SystemModstamp""".stripMargin

  def mockSf(expectedSoql: String = expectedDefaultSoql): Unit = {
    val bindingXmlResponse = SfBindingXmlResponse(endPoint, apiVersion, sfId).toString
    val describeXmlResponse = SfDescribeXmlResponse(Seq(
      SfField(ID, "string"),
      SfField(SYSTEMMODSTAMP, "datetime"),
      SfField(LASTMODIFIEDDATE, "datetime"),
      SfField(TIME_FIELD, "time"))).toString
    val sfRecordsList = Seq(
      Seq(SfRecord(ID, Some("a")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")), SfRecord(LASTMODIFIEDDATE, Some("2020-10-23T13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("b")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")), SfRecord(LASTMODIFIEDDATE, Some("2020-10-23T13:39:45.000Z"))),
      Seq(SfRecord(ID, Some("e")), SfRecord(SYSTEMMODSTAMP, Some("2020-10-23T13:39:45.000Z")), SfRecord(LASTMODIFIEDDATE, Some("2020-10-23T13:39:45.000Z")))
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
    it should "" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()

      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(AGE, IntegerType)
        ))
      val result =
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(soqlQuery) // SELECT Id,SystemModstamp FROM User WHERE isDeleted = false
          .cache

      val expected = Seq(
        (t"2020-10-23 13:39:45", "a", null.asInstanceOf[Integer]),
        (t"2020-10-23 13:39:45", "b", null.asInstanceOf[Integer]),
        (t"2020-10-23 13:39:45", "e", null.asInstanceOf[Integer])).toDF(SYSTEMMODSTAMP, ID, AGE)

      assertDataFramesEqual(expected, result)
    }

    it should "" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: *" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()
      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(NAME, StringType)
        ))
      val result =
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(selectAllSoql) // SELECT * FROM User WHERE isDeleted = false
          .cache

      val expected = Seq(
        (t"2020-10-23 13:39:45", "a", null.asInstanceOf[String]),
        (t"2020-10-23 13:39:45", "b", null.asInstanceOf[String]),
        (t"2020-10-23 13:39:45", "e", null.asInstanceOf[String])).toDF(SYSTEMMODSTAMP, ID, NAME)

      assertDataFramesEqual(expected, result)
    }

    it should "" +
      "predefined Schema: SystemModstamp, Id, Age" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()

      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(AGE, IntegerType)
        ))
      val result =
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(soqlQuery) // SELECT Id,SystemModstamp FROM User WHERE isDeleted = false
          .cache
      result.show(false)
      val expected = Seq(
        (t"2020-10-23 13:39:45", "a", null.asInstanceOf[Integer]),
        (t"2020-10-23 13:39:45", "b", null.asInstanceOf[Integer]),
        (t"2020-10-23 13:39:45", "e", null.asInstanceOf[Integer])).toDF(SYSTEMMODSTAMP, ID, AGE)

      assertDataFramesEqual(expected, result)
    }

    it should "fail if" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: Id" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()
      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(NAME, StringType)
        ))
      assertThrows[UnsupportedSchemaWithSelectException] {
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(soqlQuery)
          .select(ID)
          .show()
      }
    }

    it should "save as parquet" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      withTempDir { dir =>
        mockSf()
        val schema = StructType(Array(
          StructField(SYSTEMMODSTAMP, TimestampType),
          StructField(ID, StringType),
          StructField(NAME, StringType)
          ))
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(soqlQuery)
          .write
          .parquet(s"$dir/$SF_TABLE_NAME")

        val exp = Seq(
          (t"2020-10-23 13:39:45", "a", null.asInstanceOf[String]),
          (t"2020-10-23 13:39:45", "b", null.asInstanceOf[String]),
          (t"2020-10-23 13:39:45", "e", null.asInstanceOf[String])).toDF(SYSTEMMODSTAMP, ID, NAME)
        val res = spark.read.parquet(s"$dir/$SF_TABLE_NAME")

        assertDataFramesEqual(exp, res)
      }

    }

    it should "count" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: *" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()
      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(NAME, StringType)
        ))
      val result =
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(selectAllSoql) // SELECT * FROM User WHERE isDeleted = false
          .count

      val exp = 3
      assert(result == exp)
    }

    it should "map to Dataset" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()

      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(NAME, StringType)
        ))
      val result =
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(soqlQuery) // SELECT Id,SystemModstamp FROM User WHERE isDeleted = false
          .as[Data]
          .map(identity)
          .cache

      val expected = Seq(
        Data(t"2020-10-23 13:39:45", "a", null.asInstanceOf[String]),
        Data(t"2020-10-23 13:39:45", "b", null.asInstanceOf[String]),
        Data(t"2020-10-23 13:39:45", "e", null.asInstanceOf[String])).toDS()

      assertDatasetsEqual(expected, result)
    }

    it should "fail" +
      "predefined Schema: SystemModstamp, Id, Name" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: SystemModstamp, Id, Name" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf()

      val schema = StructType(Array(
        StructField(SYSTEMMODSTAMP, TimestampType),
        StructField(ID, StringType),
        StructField(NAME, StringType)
        ))

      assertThrows[AnalysisException] {
        getDefaultReader
          .format(sfFormat)
          .schema(schema)
          .load(soqlQuery) // SELECT Id,SystemModstamp FROM User WHERE isDeleted = false
          .as[Data2]
          .map(identity)
          .cache
      }
    }

    it should "fail" +
      "predefined Schema: None" +
      "soql: Id,SystemModstamp" +
      "sparkSelected: SystemModstamp, TimeField" in withMockedServer(wireMockServerHost, wireMockServerPort) { _ =>
      mockSf(qs"""SELECT Id,TimeField,LastModifiedDate FROM User
                 |WHERE (isDeleted = false) AND
                 |(LastModifiedDate >= 2020-01-01T00:00:00.000Z AND LastModifiedDate &lt;= 2020-01-02T00:00:00.000Z)
                 |ORDER BY LastModifiedDate""".stripMargin)

      assertThrows[AnalysisException] {
        getDefaultReader
          .option(SF_OFFSET_COL, LASTMODIFIEDDATE)
          .format(sfFormat)
          .load(soqlQuery) // SELECT Id,SystemModstamp FROM User WHERE isDeleted = false
          .select("id", TIME_FIELD)
          .cache
      }
    }

  }

  "com.keks.sf.soap.v1" should behave like tests(SALESFORCE_SOAP_V1)
  "com.keks.sf.soap.v2" should behave like tests(SALESFORCE_SOAP_V2)

}

case class Data(systemmodstamp: Timestamp,
                id: String,
                name: String)

case class Data2(systemmodstamp: Timestamp,
                 id: String,
                 name: String,
                 position: String)
