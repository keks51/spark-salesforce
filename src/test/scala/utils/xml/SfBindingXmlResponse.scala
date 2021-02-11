package utils.xml


/**
  * Xml response when creating partner connection
  * 'val soapConnection: PartnerConnection = new PartnerConnection(sfConnectionConfig)'
  * @param endPoint like 'http://localhost:18635'
  * @param apiVersion like '40.0'
  * @param id userId, organizationId, profileId, like '000000000000001'
  */
case class SfBindingXmlResponse(endPoint: String, apiVersion: String, id: String) {

  override def toString = {
    s"""<?utils.xml version="1.0" encoding="UTF-8"?>
       |<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
       |                  xmlns="urn:partner.soap.sforce.com"
       |                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
       |    <soapenv:Body>
       |        <loginResponse>
       |            <result>
       |                <metadataServerUrl>$endPoint/services/Soap/m/$apiVersion/$id
       |                </metadataServerUrl>
       |                <passwordExpired>false</passwordExpired>
       |                <sandbox>true</sandbox>
       |                <serverUrl>$endPoint/services/Soap/u/$apiVersion/$id</serverUrl>
       |                <sessionId>1</sessionId>
       |                <userId>$id</userId>
       |                <userInfo>
       |                    <accessibilityMode>false</accessibilityMode>
       |                    <currencySymbol>${"$"}</currencySymbol>
       |                    <orgAttachmentFileSizeLimit>31457280</orgAttachmentFileSizeLimit>
       |                    <orgDefaultCurrencyIsoCode>USD</orgDefaultCurrencyIsoCode>
       |                    <orgDefaultCurrencyLocale>en_US</orgDefaultCurrencyLocale>
       |                    <orgDisallowHtmlAttachments>false</orgDisallowHtmlAttachments>
       |                    <orgHasPersonAccounts>true</orgHasPersonAccounts>
       |                    <organizationId>$id</organizationId>
       |                    <organizationMultiCurrency>false</organizationMultiCurrency>
       |                    <organizationName>KEKS</organizationName>
       |                    <profileId>$id</profileId>
       |                    <roleId>00E70000000tO3NEAU</roleId>
       |                    <sessionSecondsValid>3600</sessionSecondsValid>
       |                    <userDefaultCurrencyIsoCode xsi:nil="true"/>
       |                    <userEmail>keks.keks@kgb.com</userEmail>
       |                    <userFullName>keks</userFullName>
       |                    <userId>$id</userId>
       |                    <userLanguage>en_US</userLanguage>
       |                    <userLocale>en_US</userLocale>
       |                    <userName>keks@kgb.com</userName>
       |                    <userTimeZone>America/New_York</userTimeZone>
       |                    <userType>Standard</userType>
       |                    <userUiSkin>Theme3</userUiSkin>
       |                </userInfo>
       |            </result>
       |        </loginResponse>
       |    </soapenv:Body>
       |</soapenv:Envelope>""".stripMargin
  }

}
