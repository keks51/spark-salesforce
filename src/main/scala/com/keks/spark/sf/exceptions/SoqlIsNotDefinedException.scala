package com.keks.spark.sf.exceptions

class SoqlIsNotDefinedException extends IllegalArgumentException {

  override def getMessage = {
    "SOQL query is not defined in '.load(...)'"
  }

}
