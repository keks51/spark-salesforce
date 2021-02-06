package com.keks.sf.util

import com.keks.sf.SfOptions
import com.sforce.async.{ConcurrencyMode, ContentType, JobInfo, OperationEnum}
import com.sforce.ws.ConnectorConfig


/**
  * Salesforce connection configuration utils
  */
object SfConfigUtils {

  /**
    * Building end point like 'https://login.salesforce.com/services/Soap/u/39.0'
    * @param useHttps https or http
    * @param authEndPoint like login.salesforce.com
    * @param apiVersion like 39.0
    * @return https://login.salesforce.com/services/Soap/u/39.0
    */
  def getAuthEndPoint(useHttps: Boolean,
                      authEndPoint: String,
                      apiVersion: String): String = {
    val protocol = if (useHttps) "https" else "http"
    s"$protocol://$authEndPoint/services/Soap/u/$apiVersion"
  }

  def createJobInfo(tableName: String): JobInfo = {
    val job = new JobInfo()
    job.setObject(tableName)
    job.setOperation(OperationEnum.queryAll)
    job.setContentType(ContentType.CSV)
    job.setConcurrencyMode(ConcurrencyMode.Parallel)
    job
  }

  /**
    * Creating ConnectorConfig based on query options
    * @param sfOptions query options
    */
  def createSfConnectorConfig(sfOptions: SfOptions): ConnectorConfig = {
    val config = new ConnectorConfig()
    config.setUsername(sfOptions.userName)
    config.setPassword(sfOptions.userPassword)
    config.setAuthEndpoint(getAuthEndPoint(sfOptions.useHttps, sfOptions.authEndPoint, sfOptions.apiVersion))
    config.setCompression(sfOptions.compression)
    config.setTraceMessage(sfOptions.showTrace)
    for {
      proxyHost <- sfOptions.proxyHostOpt
      proxyPort <- sfOptions.proxyPortOpt
    } yield {
      config.setProxy(proxyHost, proxyPort)
    }
    sfOptions.proxyUserNameOpt.foreach(config.setProxyUsername)
    sfOptions.proxyPasswordOpt.foreach(config.setProxyPassword)

    config.setConnectionTimeout(sfOptions.connectionTimeout)
    config.setReadTimeout(sfOptions.readTimeout)
    config.setPrettyPrintXml(sfOptions.prettyPrintXml)
    sfOptions.serviceEndPointOpt.foreach(config.setServiceEndpoint)
    sfOptions.restEndPointOpt.foreach(config.setRestEndpoint)
    sfOptions.traceFileOpt.foreach(config.setTraceFile)
    config.setMaxRequestSize(sfOptions.maxRequestSize)
    config.setMaxResponseSize(sfOptions.maxResponseSize)
    config.setValidateSchema(sfOptions.schemaValidation)
    sfOptions.ntlmDomainopt.foreach(config.setNtlmDomain)
    config.setUseChunkedPost(sfOptions.useChunkedPost)
    config.setManualLogin(sfOptions.manualLogin)
    config
  }

  def createBulkConfig(sfConnectionConfig: ConnectorConfig, apiVersion: String) = {
    val sfSessionId = sfConnectionConfig.getSessionId
    val soapEndPoint = sfConnectionConfig.getServiceEndpoint
    val config = new ConnectorConfig()
    config.setSessionId(sfSessionId)
    val restEndpoint = soapEndPoint.substring(0, soapEndPoint.indexOf("Soap/")) + "async/" + apiVersion
    config.setRestEndpoint(restEndpoint)
    config.setCompression(sfConnectionConfig.isCompression)
    config.setTraceMessage(sfConnectionConfig.isTraceMessage)
    config.setProxy(sfConnectionConfig.getProxy)
    config
  }

  /**
    * Pretty print of ConnectorConfig
    * @param conf ConnectorConfig
    * @return pretty string
    */
  def printConnectorConfig(conf: ConnectorConfig): String = {
    val get: (ConnectorConfig => Any) => Any = (f: ConnectorConfig => Any) => Option(f(conf)).map(_.toString).getOrElse("Not defined")
    s"""Cannot connect to Salesforce.
       |AuthEndPoint: '${get(_.getAuthEndpoint)}'
       |ServiceEndPoint: '${get(_.getServiceEndpoint)}'
       |RestEndPoint: '${get(_.getRestEndpoint)}'
       |UserName: '${get(_.getUsername)}'
       |IsCompression: '${get(_.isCompression)}'
       |ConnectionTimeout: '${get(_.getConnectionTimeout)}'
       |Proxy: '${get(_.getProxy)}'
       |ProxyUserName: '${get(_.getProxyUsername)}'
       |ProxyPassword: '${Option(conf.getProxyUsername).map(_ => "Defined but is secret").getOrElse("Not defined")}'
       |""".stripMargin
  }

}
