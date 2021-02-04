package com.keks.sf.soap.v2.streaming

import org.apache.spark.sql.sources.v2.reader.streaming.Offset


case class SfOffset(offset: String) extends Offset {

  override def json: String = offset

}

object SfOffset {

  def apply(offset: Offset): SfOffset = SfOffset(offset.json())

  val empty: SfOffset = null.asInstanceOf[SfOffset]

}
