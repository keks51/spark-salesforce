package com.keks.spark.sf.exceptions

import com.keks.spark.sf.util.SfConfigUtils
import com.sforce.ws.{ConnectionException, ConnectorConfig}


class SalesforceConnectionException(conf: ConnectorConfig) extends ConnectionException {

  override def getMessage = SfConfigUtils.printConnectorConfig(conf)

}
