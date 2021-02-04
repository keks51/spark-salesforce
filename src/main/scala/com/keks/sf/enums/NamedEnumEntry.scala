package com.keks.sf.enums

import enumeratum.EnumEntry


trait NamedEnumEntry extends EnumEntry with Serializable  {
  val name: String
  override def toString: String = name
}
