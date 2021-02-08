package com.keks.spark.sf

import org.apache.spark.sql.catalyst.InternalRow


/**
  * Like Jdbc result set
  */
trait SfResultSet extends LogSupport {

  def hasNext: Boolean

  def getRow: (Boolean, InternalRow)

  def getCurrentTime: Long = System.currentTimeMillis()

}

object SfResultSet {

  def empty: SfResultSet =
    new SfResultSet {
      override def hasNext = false

      override def getRow: (Boolean, InternalRow) = (true, null.asInstanceOf[InternalRow])
    }

}
