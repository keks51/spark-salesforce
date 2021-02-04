package com.keks.sf.util

import scala.util.Random


object Utils {

  def getRandomDelay(min: Int, max: Int): Int = {
    math.ceil(min + ((max - min) * Random.nextDouble)).toInt
  }

}
