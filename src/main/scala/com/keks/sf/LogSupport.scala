package com.keks.sf

import org.apache.log4j.Logger


trait LogSupport {
  val toConsole = true
  @transient private lazy val log: Logger = org.apache.log4j.LogManager.getLogger(getClass)

  def info(text: String): Unit = if (toConsole) println(s"INFO: $text") else log.info(text)
  def debug(text: String): Unit = if (toConsole) println(s"DEBUG: $text") else log.debug(text)
  def warn(text: String): Unit = if (toConsole) println(s"WARN: $text") else log.warn(text)
  def error(text: String): Unit = if (toConsole) System.err.println(s"ERROR: $text") else log.error(text)
  def error(text: String, t: Throwable): Unit = if (toConsole) System.err.println(s"ERROR: $text") else log.error(text, t)

}
