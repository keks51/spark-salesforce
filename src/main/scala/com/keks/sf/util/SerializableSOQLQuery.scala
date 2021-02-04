package com.keks.sf.util

import org.mule.tools.soql.SOQLParserHelper
import org.mule.tools.soql.query.SOQLQuery

import java.io.{ObjectInputStream, ObjectOutputStream}


case class SerializableSOQLQuery(var soql: SOQLQuery) {

  private def writeObject(out: ObjectOutputStream): Unit = out.writeUTF(soql.toSOQLText)

  private def readObject(in: ObjectInputStream): Unit = soql = SOQLParserHelper.createSOQLData(in.readUTF())

}
