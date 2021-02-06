package com.keks.sf.util

import java.util.concurrent.atomic.AtomicInteger


case class UniqueQueryId(id: Int) {

  override def toString = id.toString

}

object UniqueQueryId {

  private val uniqueQueryId = new AtomicInteger(0)

  def getUniqueQueryId: UniqueQueryId = UniqueQueryId(uniqueQueryId.getAndIncrement())

}
