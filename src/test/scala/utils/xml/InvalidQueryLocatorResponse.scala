package utils.xml


case class InvalidQueryLocatorResponse() {

  override def toString = {
    """<?utils.xml version="1.0" encoding="UTF-8"?>
      |<soapenv:Envelope
      |        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
      |        xmlns:sf="urn:fault.partner.soap.sforce.com"
      |        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      |    <soapenv:Body>
      |        <soapenv:Fault>
      |            <faultcode>sf:INVALID_QUERY_LOCATOR</faultcode>
      |            <faultstring>INVALID_QUERY_LOCATOR: invalid query locator</faultstring>
      |            <detail>
      |                <sf:InvalidQueryLocatorFault xsi:type="sf:InvalidQueryLocatorFault">
      |                    <sf:exceptionCode>INVALID_QUERY_LOCATOR</sf:exceptionCode>
      |                    <sf:exceptionMessage>invalid query locator</sf:exceptionMessage>
      |                </sf:InvalidQueryLocatorFault>
      |            </detail>
      |        </soapenv:Fault>
      |    </soapenv:Body>
      |</soapenv:Envelope>""".stripMargin
  }

}
