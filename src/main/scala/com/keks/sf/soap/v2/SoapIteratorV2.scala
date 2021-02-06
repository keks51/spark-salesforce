package com.keks.sf.soap.v2

import com.keks.sf.SfResultSet
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader


/**
  * Spark iterator for soap result set.
  *
  * @param rs SfResultSet implementation.
  */
class SoapIteratorV2(rs: SfResultSet) extends InputPartitionReader[InternalRow] {

  private var gotNext = false
  private var nextValue: InternalRow = _
  protected var finished = false

  override def next(): Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext
        gotNext = true
      }
    }
    !finished
  }

  override def close(): Unit = {}

  private def getNext: InternalRow = {
    if (rs.hasNext) {
      val (isEmptyRow, row) = rs.getRow
      finished = isEmptyRow
      row
    } else {
      finished = true
      null.asInstanceOf[InternalRow]
    }
  }

  override def get(): InternalRow = {
    if (!next) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }

}
