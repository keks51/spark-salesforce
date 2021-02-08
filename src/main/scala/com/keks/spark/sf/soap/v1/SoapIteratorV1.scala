package com.keks.spark.sf.soap.v1

import com.keks.spark.sf.SfResultSet
import org.apache.spark.sql.catalyst.InternalRow


/**
  * Spark iterator for soap result set.
  *
  * @param rs SfResultSet implementation.
  */
case class SoapIteratorV1(rs: SfResultSet) extends Iterator[InternalRow] {

  private var gotNext = false
  private var nextValue: InternalRow = _
  protected var finished = false

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

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext
        gotNext = true
      }
    }
    !finished
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }

}
