package com.keks.sf.exceptions

import com.keks.sf.util.SfConfigUtils
import com.sforce.ws.{ConnectionException, ConnectorConfig}


class SalesforceConnectionException(conf: ConnectorConfig) extends ConnectionException {

  override def getMessage = SfConfigUtils.printConnectorConfig(conf)

}
