package com.keks.spark.sf.enums

import enumeratum.Enum

sealed abstract class SparkNode(val name: String) extends NamedEnumEntry

object SparkNode extends Enum[SparkNode] {

  case object DRIVER extends SparkNode("DRIVER")
  case object EXECUTOR extends SparkNode("EXECUTOR")

  override def values = findValues

}
