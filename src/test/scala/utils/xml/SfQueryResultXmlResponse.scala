package utils.xml

/**
  * Xml response when querying records
  *
  * @param sfTableName   salesforce table name like 'User'
  * @param sfRecordsList list of rows (batch of records for one row)
  */
case class SfQueryResultXmlResponse(sfTableName: String,
                                    sfRecordsList: Seq[Seq[SfRecord]] = Seq.empty[Seq[SfRecord]],
                                    queryLocator: Option[String] = None,
                                    queryMoreResponse: Boolean = false,
                                    globalRecordsNumber: Long = 4L) {

  val containsId: Boolean = sfRecordsList.headOption.exists(_.map(_.name).contains("Id"))

  override def toString = {
    s"""<?utils.xml version="1.0" encoding="UTF-8"?>
       |<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:partner.soap.sforce.com"
       |                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:sf="urn:sobject.partner.soap.sforce.com">
       |    <soapenv:Header>
       |        <LimitInfoHeader>
       |            <limitInfo>
       |                <current>74343</current>
       |                <limit>21288800</limit>
       |                <type>API REQUESTS</type>
       |            </limitInfo>
       |        </LimitInfoHeader>
       |    </soapenv:Header>
       |    <soapenv:Body>
       |        ${if (queryMoreResponse) "<queryMoreResponse>" else "<queryAllResponse>"}
       |            <result xsi:type="QueryResult">
       |                <done>${queryLocator.isEmpty}</done>
       |                ${queryLocator.map(value => s"<queryLocator>$value</queryLocator>").getOrElse("""<queryLocator xsi:nil="true"/>""")}
       |                ${if (sfRecordsList.isEmpty) "" else sfRecordsList.map(e => SfRecords(sfTableName, containsId, e).toString).mkString("\n")}
       |                <size>$globalRecordsNumber</size>
       |            </result>
       |        ${if (queryMoreResponse) "</queryMoreResponse>" else "</queryAllResponse>"}
       |    </soapenv:Body>
       |</soapenv:Envelope>""".stripMargin
  }

}

/**
  * Creating sf record like
  * '<sf:SystemModstamp>1</sf:SystemModstamp>'
  * when record value is None (means null)
  * '<sf:Voice_Mail_nov_c xsi:nil="true"/>'
  * For id column record should be duplicated
  *
  * @param name  record name
  * @param value record value
  */
case class SfRecord(name: String, value: Option[String]) {


  override def toString = {
    (name, value) match {
      case ("Id", Some(idValue)) =>
        s"""<sf:Id>$idValue</sf:Id>
           |<sf:Id>$idValue</sf:Id>""".stripMargin
      case (_, Some(fieldValue)) =>
        s"<sf:$name>$fieldValue</sf:$name>"
      case _ =>
        s"<sf:$name xsi:nil=${"\"true\""}/>"
    }
  }

}

/**
  * Creating list of records for one row.
  * if records don't contain id column it should be added with null value
  *
  * @param sfTableName sf table name like 'User'
  * @param containsId  do records contain Id?
  * @param records     list of records
  */
case class SfRecords(sfTableName: String,
                     containsId: Boolean,
                     records: Seq[SfRecord]) {

  override def toString = {
    s"""                    <records xsi:type="sf:sObject">
     |                        <sf:type>$sfTableName</sf:type>${if (containsId) "" else "\n    <sf:Id xsi:nil=\"true\"/>"}
     |                        ${records.mkString("\n")}
     |                    </records>""".stripMargin
  }

}
