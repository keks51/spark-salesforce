package utils.xml


/**
  * Xml response when requesting 'describe' action
  * 'soapConnection.describeSObject(tableName)'
  * @param fields list of fields
  */
case class SfDescribeXmlResponse(fields: Seq[SfField]) {

  override def toString = {
    s"""<?utils.xml version="1.0" encoding="UTF-8"?>
       |<soapenv:Envelope
       |    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
       |    xmlns="urn:partner.soap.sforce.com"
       |    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
       |    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
       |    <soapenv:Header>
       |        <LimitInfoHeader>
       |            <limitInfo>
       |                <current>52903</current>
       |                <limit>21288800</limit>
       |                <type>API REQUESTS</type>
       |            </limitInfo>
       |        </LimitInfoHeader>
       |    </soapenv:Header>
       |    <soapenv:Body>
       |        <describeSObjectResponse>
       |            <result>
       |            ${fields.mkString("\n")}
       |            </result>
       |        </describeSObjectResponse>
       |    </soapenv:Body>
       |</soapenv:Envelope>""".stripMargin
  }



}

/**
  *
  * @param fieldName like 'id', 'isDeleted' ...
  * @param fieldType reference, boolean,
  */
case class SfField(fieldName: String, fieldType: String) {

  override def toString = {
    s"""    <fields>
       |                    <name>$fieldName</name>
       |                    <type>$fieldType</type>
       |                </fields>""".stripMargin
  }

}
