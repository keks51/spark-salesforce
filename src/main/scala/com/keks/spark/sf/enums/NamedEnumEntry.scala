package com.keks.spark.sf.enums

import enumeratum.EnumEntry


trait NamedEnumEntry extends EnumEntry with Serializable  {
  val name: String
  override def toString: String = name
}
