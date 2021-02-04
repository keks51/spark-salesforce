package com.keks.sf.enums

import enumeratum.Enum


sealed abstract class PartitionColType(val name: String) extends NamedEnumEntry

object PartitionColType extends Enum[SoapDelivery] {

  case object ISO_DATE_TIME extends PartitionColType("ISO_DATE_TIME")
  case object INTEGER extends PartitionColType("INTEGER")
  case object DOUBLE extends PartitionColType("DOUBLE")
  case object STRING extends PartitionColType("STRING")

  override def values = findValues

}

