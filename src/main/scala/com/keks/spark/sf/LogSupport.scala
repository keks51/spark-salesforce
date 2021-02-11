package com.keks.spark.sf

import com.keks.spark.sf.util.UniqueQueryId
import org.apache.log4j.Logger


trait LogSupport {

  // printing to console for debugging
  private val toConsole = false
  @transient private lazy val log: Logger = org.apache.log4j.LogManager.getLogger(getClass)

  def info(text: String): Unit = if (toConsole) println(s"INFO: $text") else log.info(text)

  def debug(text: String): Unit = if (toConsole) println(s"DEBUG: $text") else log.debug(text)

  def warn(text: String): Unit = if (toConsole) println(s"WARN: $text") else log.warn(text)

  def error(text: String): Unit = if (toConsole) System.err.println(s"ERROR: $text") else log.error(text)

  def error(text: String, t: Throwable): Unit = if (toConsole) System.err.println(s"ERROR: $text") else log.error(text, t)


  def infoQ(text: String)(implicit id: UniqueQueryId): Unit =
    if (toConsole) println(s"QueryId:$id. INFO: $text") else log.info(s"QueryId:$id.$text")

  def debugQ(text: String)(implicit id: UniqueQueryId): Unit =
    if (toConsole) println(s"QueryId:$id. DEBUG: $text") else log.debug(s"QueryId:$id.$text")

  def warnQ(text: String)(implicit id: UniqueQueryId): Unit =
    if (toConsole) println(s"QueryId:$id. WARN: $text") else log.warn(s"QueryId:$id.$text")

  def errorQ(text: String)(implicit id: UniqueQueryId): Unit =
    if (toConsole) System.err.println(s"QueryId:$id. ERROR: $text") else log.error(s"QueryId:$id.$text")

  def errorQ(text: String, t: Throwable)(implicit id: UniqueQueryId): Unit =
    if (toConsole) System.err.println(s"QueryId:$id. ERROR: $text") else log.error(s"QueryId:$id.$text", t)

}
