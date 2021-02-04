package com.keks.sf

import org.mule.tools.soql.SOQLParserHelper
import org.mule.tools.soql.query.SOQLQuery

import scala.util.{Failure, Success, Try}


object implicits {

  implicit class RichTry[T](`try`: Try[T]) {

    def onFailure(f: Throwable => T): T = {
      `try` match {
        case Success(value) => value
        case Failure(exception) => f(exception)
      }
    }

  }

  implicit class RichStrArray(arr: Array[String]) {

    def containsIgnoreCaseEx(rightArr: Array[String])(notExistElem: String => Unit): Unit = {
      val lowerArr = arr.map(_.toLowerCase)
      val lowerRightArr = rightArr.map(e => (e, e.toLowerCase))
      lowerRightArr.foreach { case (elem, lowerElem) => lowerArr.find(_ == lowerElem).getOrElse(notExistElem(elem)) }
    }

    def containsIgnoreCaseEx(elem: String)(notExistElem: => Unit): Unit = {
      arr.map(_.toLowerCase).find(_ == elem.toLowerCase).getOrElse(notExistElem)
    }

    def containsIgnoreCase(elem: String): Boolean = {
      arr.map(_.toLowerCase).contains(elem.toLowerCase)
    }

  }

  implicit class RichSOQL(soql: SOQLQuery) {

    def copySOQL: SOQLQuery = {
      SOQLParserHelper.createSOQLData(soql.toSOQLText)
    }

  }


}
