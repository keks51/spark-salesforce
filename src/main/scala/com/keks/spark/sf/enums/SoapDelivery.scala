package com.keks.spark.sf.enums

import enumeratum.Enum


sealed abstract class SoapDelivery(val name: String) extends NamedEnumEntry

object SoapDelivery extends Enum[SoapDelivery] {

  case object AT_LEAST_ONCE extends SoapDelivery("AT_LEAST_ONCE")
  case object AT_MOST_ONCE extends SoapDelivery("AT_MOST_ONCE")

  override def values = findValues

}
