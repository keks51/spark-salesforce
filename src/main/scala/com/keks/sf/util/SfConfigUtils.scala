package com.keks.sf.util

import com.keks.sf.SfOptions
import com.sforce.async.{ConcurrencyMode, ContentType, JobInfo, OperationEnum}
import com.sforce.ws.ConnectorConfig


object SfConfigUtils {

  val authEndPointBuilder: (Boolean, String, String) => String =
    (useHttps, authEndPoint: String, apiVersion: String) => {
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

  def createSfConnectorConfig(sfOptions: SfOptions): ConnectorConfig = {
    val config = new ConnectorConfig()
    config.setUsername(sfOptions.userName)
    config.setPassword(sfOptions.userPassword)
    config.setAuthEndpoint(authEndPointBuilder(sfOptions.useHttps, sfOptions.authEndPoint, sfOptions.apiVersion))
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

  def createSfConnectorConfig(userName: String,
                              userPassword: String,
                              useHttps: Boolean = true,
                              authEndPoint: String,
                              apiVersion: String,
                              enableCompression: Boolean,
                              enableShowTrace: Boolean,
                              proxyHostOpt: Option[String],
                              proxyPortOpt: Option[Int],
                              connectionTimeout: Int = 30000): ConnectorConfig = {
    val config = new ConnectorConfig()
    config.setConnectionTimeout(connectionTimeout)
    config.setUsername(userName)
    config.setPassword(userPassword)
    config.setAuthEndpoint(authEndPointBuilder(useHttps, authEndPoint, apiVersion))
    config.setCompression(enableCompression)
    config.setTraceMessage(enableShowTrace)
    //        config.setSessionRenewer(sessionRenewer)
    for {
      proxyHost <- proxyHostOpt
      proxyPort <- proxyPortOpt
    } yield {
      config.setProxy(proxyHost, proxyPort)
    }
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
