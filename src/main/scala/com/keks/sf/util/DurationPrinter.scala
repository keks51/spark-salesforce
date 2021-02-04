package com.keks.sf.util


sealed trait DurationPrinter[T] {

  def maxBound(millis: Long): Long

  def unBound(millis: Long): Long

  def print(value: Long): String

  def getTime(value: Long, first: Boolean): Long = {
    if (first) unBound(value) else maxBound(value)
  }

}

/**
  * Usage:
  * D -> days
  * H -> hours
  * M -> minutes
  * S -> seconds
  * MS -> milliseconds
  * {{{
  *   import LetterTimeUnit._
  *   val duration1 = Duration(1000020001, TimeUnit.MILLISECONDS)
  *   DurationPrinter.print[D, H, M, S, MS](duration1)
  *   res0 = 11d 13h 47min 0sec 1millis
  *   val duration2 = Duration(5000, TimeUnit.MILLISECONDS)
  *   DurationPrinter.print[H, M, S](duration2)
  *   res1 = 5sec
  *   DurationPrinter.print[D, H](duration1)
  *   res2 = 11d 13h
  * }}}
  *
  *
  */
object DurationPrinter {

  import com.keks.sf.util.LetterTimeUnit._


  type TP[T] = DurationPrinter[T]

  implicit val ms: TP[MS] = new TP[MS] {
    override def maxBound(millis: Long) = unBound(millis) % 1000

    override def unBound(millis: Long) = millis

    override def print(value: Long) = s"${value}millis"
  }

  implicit val s: TP[S] = new TP[S] {
    override def maxBound(millis: Long) = unBound(millis) % 60

    override def unBound(millis: Long) = millis / 1000

    override def print(value: Long) = s"${value}sec"
  }

  implicit val m: TP[M] = new TP[M] {
    override def maxBound(millis: Long) = unBound(millis) % 60

    override def unBound(millis: Long) = (millis / 1000) / 60

    override def print(value: Long) = s"${value}min"
  }

  implicit val h: TP[H] = new TP[H] {
    override def maxBound(millis: Long) = unBound(millis) % 24

    override def unBound(millis: Long) = ((millis / 1000) / 60) / 60

    override def print(value: Long) = s"${value}h"
  }

  implicit val d: TP[D] = new TP[D] {
    override def maxBound(millis: Long) = 0

    override def unBound(millis: Long) = ((millis / 1000) / 60) / 60 / 24

    override def print(value: Long) = s"${value}d"
  }

  private def print[T1](millis: Long, acc: Seq[String])(implicit enc1: TP[T1]): String = {
    val res = enc1.getTime(millis, acc.isEmpty)
    (acc :+ enc1.print(res)).mkString(" ")
  }

  private def print[T1, T2: TP](millis: Long, acc: Seq[String])(implicit enc1: TP[T1]): String = {
    val res = enc1.getTime(millis, acc.isEmpty)
    val newAcc = if (acc.isEmpty && res == 0) acc else acc :+ enc1.print(res)
    print[T2](millis, newAcc)
  }

  private def print[T1, T2: TP, T3: TP](millis: Long, acc: Seq[String])(implicit enc1: TP[T1]): String = {
    val res = enc1.getTime(millis, acc.isEmpty)
    val newAcc = if (acc.isEmpty && res == 0) acc else acc :+ enc1.print(res)
    print[T2, T3](millis, newAcc)
  }

  private def print[T1, T2: TP, T3: TP, T4: TP](millis: Long, acc: Seq[String])(implicit enc1: TP[T1]): String = {
    val res = enc1.getTime(millis, acc.isEmpty)
    val newAcc = if (acc.isEmpty && res == 0) acc else acc :+ enc1.print(res)
    print[T2, T3, T4](millis, newAcc)
  }

  private def print[T1, T2: TP, T3: TP, T4: TP, T5: TP](millis: Long, acc: Seq[String])(implicit enc1: TP[T1]): String = {
    val res = enc1.getTime(millis, acc.isEmpty)
    val newAcc = if (acc.isEmpty && res == 0) acc else acc :+ enc1.print(res)
    print[T2, T3, T4, T5](millis, newAcc)
  }

  def print[T1: TP](value: Long): String = print[T1](value, Seq.empty)

  def print[T1: TP, T2 <: TP[T1] : TP](value: Long): String = print[T1, T2](value, Seq.empty)

  def print[T1: TP, T2 <: TP[T1] : TP, T3 <: TP[T2] : TP](value: Long): String = print[T1, T2, T3](value, Seq.empty)

  def print[T1: TP, T2 <: TP[T1] : TP, T3 <: TP[T2] : TP, T4 <: TP[T3] : TP](value: Long): String = print[T1, T2, T3, T4](value, Seq.empty)

  def print[T1: TP, T2 <: TP[T1] : TP, T3 <: TP[T2] : TP, T4 <: TP[T3] : TP, T5 <: TP[T4] : TP](value: Long): String = print[T1, T2, T3, T4, T5](value, Seq.empty)

}

object LetterTimeUnit {

  trait MS extends DurationPrinter[S]

  trait S extends DurationPrinter[M]

  trait M extends DurationPrinter[H]

  trait H extends DurationPrinter[D]

  trait D

}
