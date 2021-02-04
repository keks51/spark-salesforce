package com.keks.sf

import com.keks.sf.implicits._
import utils.TestBase

import scala.util.Try


class implicitsTest extends TestBase {

  "RichTry#onFailure" should "be not executed" in {
    val data = 1
    val res = Try(data).onFailure(e => throw e)
    assert(res == data)
  }

  "RichTry#onFailure" should "throw exception" in {
    assertThrows[NullPointerException](Try(throw new NullPointerException()).onFailure(e => throw e))
  }

}
