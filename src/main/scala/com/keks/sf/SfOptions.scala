package com.keks.sf

import com.keks.sf.SfOptions._
import com.keks.sf.exceptions.SoqlIsNotDefinedException
import com.keks.sf.soap.DEFAULT_SOAP_QUERY_EXECUTOR_CLASS
import com.keks.sf.util.SoqlUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.v2.DataSourceOptions

import java.util.Locale
import scala.collection.mutable
import scala.util.Try


class SfOptions(@transient private val parameters: CaseInsensitiveMap[String],
                sparkConf: SparkConf) extends LogSupport with Serializable {

  private val existsInOptionsOrSparkConf: String => Unit =
    (optionName: String) =>
      require(parameters.isDefinedAt(optionName)
                || sparkConf.getOption(optionName).isDefined
                || sparkConf.getOption(s"spark.sf.$optionName").isDefined, s"Option '$optionName' is required.")

  private val existsInOptions: String => Unit =
    (optionName: String) =>
      require(parameters.isDefinedAt(optionName), s"Option '$optionName' is required.")

  private val getFromOptionsOrSparkConf: String => String = (optionName: String) =>
    parameters.get(optionName).orElse(sparkConf.getOption(optionName)).getOrElse(sparkConf.get(s"spark.$optionName"))
  private val getFromOptionsOrSparkConfOpt: String => Option[String] = (optionName: String) =>
    parameters.get(optionName).orElse(sparkConf.getOption(optionName)).orElse(sparkConf.getOption(s"spark.$optionName"))

  private val getFromOptions: String => String = (optionName: String) =>
    parameters(optionName)

  private val paths: Option[String] = parameters.get(DataSourceOptions.PATHS_KEY)
  require(paths.map(_ == "[]").getOrElse(paths.isEmpty), "Only one SOQL query should be defined in '.load(...)'")
  val soql: String = parameters
    .get(DataSourceOptions.PATH_KEY)
    .map(e => if (e.trim.contains(" ")) e else s"SELECT * FROM $e") // check if .load("User")
    .getOrElse(throw new SoqlIsNotDefinedException)

  val isQueryAll = getFromOptionsOrSparkConfOpt(SF_IS_QUERY_ALL).forall { e => isBool(SF_IS_QUERY_ALL, e); e.toBoolean }

  val isSelectAll = SoqlUtils.isSelectAll(soql)

  existsInOptionsOrSparkConf(SF_API_VERSION)
  val apiVersion: String = getFromOptionsOrSparkConf(SF_API_VERSION)

  val compression: Boolean = getFromOptionsOrSparkConfOpt(SF_COMPRESSION).exists { e => isBool(SF_COMPRESSION, e); e.toBoolean }

  // exists means False by default
  val showTrace: Boolean = getFromOptionsOrSparkConfOpt(SF_SHOW_TRACE).exists { e => isBool(SF_SHOW_TRACE, e); e.toBoolean }

  val proxyHostOpt: Option[String] =
    getFromOptionsOrSparkConfOpt(SF_PROXY_HOST)
  val proxyPortOpt: Option[Int] =
    getFromOptionsOrSparkConfOpt(SF_PROXY_PORT)
      .map { portValue => isInt(SF_PROXY_PORT, portValue); portValue.toInt }

  val proxyUserNameOpt: Option[String] =
    getFromOptionsOrSparkConfOpt(SF_PROXY_USERNAME)

  val proxyPasswordOpt: Option[String] =
    getFromOptionsOrSparkConfOpt(SF_PROXY_PASSWORD)

  existsInOptionsOrSparkConf(SF_USER_NAME)
  val userName = getFromOptionsOrSparkConf(SF_USER_NAME)

  existsInOptionsOrSparkConf(SF_USER_PASSWORD)
  val userPassword: String = getFromOptionsOrSparkConf(SF_USER_PASSWORD)

  existsInOptionsOrSparkConf(SF_AUTH_END_POINT)
  val authEndPoint: String = getFromOptionsOrSparkConfOpt(SF_AUTH_END_POINT).getOrElse("login.salesforce.com")

  val useHttps: Boolean = getFromOptionsOrSparkConfOpt(SF_USE_HTTPS).forall { e => isBool(SF_USE_HTTPS, e); e.toBoolean }


  val connectionTimeout: Int = getFromOptionsOrSparkConfOpt(SF_CONNECTION_TIMEOUT)
    .map { e => isInt(SF_CONNECTION_TIMEOUT, e); e.toInt }.getOrElse(30000)

  val readTimeout = getFromOptionsOrSparkConfOpt(SF_READ_TIMEOUT)
    .map { e => isInt(SF_CONNECTION_TIMEOUT, e); e.toInt }.getOrElse(0)

  val prettyPrintXml: Boolean = getFromOptionsOrSparkConfOpt(SF_PRETTY_PRINT_XML).exists { e => isBool(SF_PRETTY_PRINT_XML, e); e.toBoolean }
  val serviceEndPointOpt: Option[String] = getFromOptionsOrSparkConfOpt(SF_SERVICE_ENDPOINT)
  val restEndPointOpt: Option[String] = getFromOptionsOrSparkConfOpt(SF_REST_ENDPOINT)
  val traceFileOpt: Option[String] = getFromOptionsOrSparkConfOpt(SF_TRACE_FILE)
  val maxRequestSize: Int = getFromOptionsOrSparkConfOpt(SF_MAX_REQUEST_SIZE)
    .map { e => isInt(SF_MAX_REQUEST_SIZE, e); e.toInt }.getOrElse(0)
  val maxResponseSize: Int = getFromOptionsOrSparkConfOpt(SF_MAX_RESPONSE_SIZE)
    .map { e => isInt(SF_MAX_RESPONSE_SIZE, e); e.toInt }.getOrElse(0)

  val schemaValidation: Boolean = getFromOptionsOrSparkConfOpt(SF_PRETTY_PRINT_XML).forall { e => isBool(SF_PRETTY_PRINT_XML, e); e.toBoolean }

  val ntlmDomainopt: Option[String] = getFromOptionsOrSparkConfOpt(SF_NTLM_DOMAIN)

  val useChunkedPost: Boolean = getFromOptionsOrSparkConfOpt(SF_USE_CHUNKED_POST).exists { e => isBool(SF_USE_CHUNKED_POST, e); e.toBoolean }
  val manualLogin: Boolean = getFromOptionsOrSparkConfOpt(SF_MANUAL_LOGIN).exists { e => isBool(SF_MANUAL_LOGIN, e); e.toBoolean }

  val queryExecutorClassName = getFromOptionsOrSparkConfOpt(SOAP_QUERY_EXECUTOR_CLASS_NAME).getOrElse(DEFAULT_SOAP_QUERY_EXECUTOR_CLASS)

  val offsetColumn: String = getFromOptionsOrSparkConfOpt(SF_OFFSET_COL).getOrElse {
    info(s"For option: '$SF_OFFSET_COL' default offset column 'SystemModstamp' is used.")
    "SystemModstamp"
  }

  val initialOffsetOpt: Option[String] = getFromOptionsOrSparkConfOpt(SF_INITIAL_OFFSET)

  val sfEndOffsetOpt: Option[String] = getFromOptionsOrSparkConfOpt(SF_END_OFFSET)
  val sfLoadNumPartitions: Int = getFromOptionsOrSparkConfOpt(SF_LOAD_NUM_PARTITIONS).map { e => isInt(SF_LOAD_NUM_PARTITIONS, e); e.toInt }.getOrElse(1)

  val checkConnectionRetries: Int = getFromOptionsOrSparkConfOpt(SF_CHECK_CONNECTION_RETRIES)
    .map { e => isInt(SF_CHECK_CONNECTION_RETRIES, e); e.toInt }.getOrElse(10)

  val checkConnectionRetrySleepMin: Int = getFromOptionsOrSparkConfOpt(SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MIN)
    .map { e => isInt(SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MIN, e); e.toInt }.getOrElse(5000)

  val checkConnectionRetrySleepMax: Int = getFromOptionsOrSparkConfOpt(SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MAX)
    .map { e => isInt(SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MAX, e); e.toInt }.getOrElse(20000)

  val streamingMaxBatches: Int = getFromOptionsOrSparkConfOpt(SF_STREAMING_MAX_BATCHES)
    .map { e => isInt(SF_STREAMING_MAX_BATCHES, e); e.toInt }.getOrElse(1000)

  val streamingMaxRecordsInPartition: Int = getFromOptionsOrSparkConfOpt(SF_STREAMING_MAX_RECORDS_IN_PARTITIONS)
    .map { e => isInt(SF_STREAMING_MAX_RECORDS_IN_PARTITIONS, e); e.toInt }.getOrElse(10000)

  val streamingAdditionalWaitWhenIncrementalLoading: Int = getFromOptionsOrSparkConfOpt(SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING)
    .map { e => isInt(SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING, e); e.toInt }.getOrElse(60000)

  val streamingLoadAvailableData = getFromOptionsOrSparkConfOpt(SF_STREAMING_LOAD_AVAILABLE_DATA)
    .exists { e => isBool(SF_STREAMING_LOAD_AVAILABLE_DATA, e); e.toBoolean }

  val streamingQueryNameOpt = getFromOptionsOrSparkConfOpt(SF_STREAMING_QUERY_NAME)

}

object SfOptions {


  val isBool: (String, String) => Unit =
    (optionName: String, value: String) => require(Try(value.toBoolean).toOption.isDefined, s"Option '$optionName' should be Boolean.")

  val isInt: (String, String) => Unit =
    (optionName: String, value: String) => require(Try(value.toInt).toOption.isDefined, s"Option '$optionName' should be Int.")

  private val sfOptionNames: mutable.Set[String] = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    sfOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  def apply(parameters: Map[String, String], sparkConf: SparkConf): SfOptions = {
    new SfOptions(CaseInsensitiveMap(parameters), sparkConf)
  }

  val SF_USER_NAME = newOption("userName")
  val SF_USER_PASSWORD = newOption("userPassword")
  val SF_AUTH_END_POINT = newOption("authEndPoint")
  val SF_API_VERSION = newOption("apiVersion")
  val SF_PROXY_HOST = newOption("proxyHost")
  val SF_PROXY_PORT = newOption("proxyPort")
  val SF_PROXY_USERNAME = newOption("proxyUserName")
  val SF_PROXY_PASSWORD = newOption("proxyPassword")
  val SF_USE_HTTPS = newOption("useHttps")
  val SF_CONNECTION_TIMEOUT = newOption("connectionTimeout")
  val SF_IS_QUERY_ALL = newOption("isQueryAll")
  val SF_CHECK_CONNECTION_RETRIES = newOption("checkConnectionRetries")
  val SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MIN = newOption("retrySleepMin")
  val SF_CHECK_CONNECTION_RETRY_SLEEP_MILLIS_MAX = newOption("retrySleepMax")



  val SF_OFFSET_COL = newOption("offsetColumn")
  val SF_INITIAL_OFFSET = newOption("initialOffset")
  val SF_END_OFFSET = newOption("endOffset")
  val SF_LOAD_NUM_PARTITIONS = newOption("loadNumPartitions")
  val SOAP_QUERY_EXECUTOR_CLASS_NAME = newOption("queryExecutorClassName")
  val SF_STREAMING_MAX_RECORDS_IN_PARTITIONS = newOption("streamingMaxRecordsInPartition")
  val SF_STREAMING_MAX_BATCHES = newOption("streamingMaxBatches")
  val SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING = newOption("streamingAdditionalWaitWhenIncrementalLoading")
  val SF_STREAMING_LOAD_AVAILABLE_DATA = newOption("streamingLoadAvailableData")
  val SF_STREAMING_QUERY_NAME = newOption("streamingQueryName")



  val SF_COMPRESSION = newOption("compression")
  val SF_SHOW_TRACE = newOption("showTrace")
  val SF_READ_TIMEOUT = newOption("readTimeout")
  val SF_PRETTY_PRINT_XML = newOption("prettyPrintXml")
  val SF_SERVICE_ENDPOINT = newOption("serviceEndPoint")
  val SF_REST_ENDPOINT = newOption("restEndPoint")
  val SF_TRACE_FILE = newOption("traceFile")
  val SF_MAX_REQUEST_SIZE = newOption("maxRequestSize")
  val SF_MAX_RESPONSE_SIZE = newOption("maxResponseSize")
  val SF_NTLM_DOMAIN = newOption("ntlmDomainopt")
  val SF_USE_CHUNKED_POST = newOption("useChunkedPost") // false
  val SF_MANUAL_LOGIN = newOption("manualLogin") // false
}
