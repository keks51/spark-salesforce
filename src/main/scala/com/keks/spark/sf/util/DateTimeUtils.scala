package com.keks.spark.sf.util

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}


object DateTimeUtils {

  val JAVA_SQL_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS"

  /* 2020-10-23T13:39:45.000Z -> 2020-10-23 13:39:45.0 */
  def isoStrToTimestamp(str: String): Timestamp = {
    Timestamp.valueOf(LocalDateTime.ofEpochSecond(Instant.parse(str).getEpochSecond, 0, ZoneOffset.UTC))
  }

  /* @example if input is '2018-05-01 00:00:00.000' then return is '2018-05-01T00:00:00.000Z' */
  def parseSqlTimestampAsStringToDate(timestampStr: String): DateTime = {
    parseDateByPattern(JAVA_SQL_TIMESTAMP_PATTERN, timestampStr)
  }
  private def parseDateByPattern(pattern: String, date: String): DateTime = {
    val formatter = DateTimeFormat.forPattern(pattern).withZone(DateTimeZone.UTC)
    formatter.parseDateTime(date)
  }

}
