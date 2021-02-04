package com.keks.sf.exceptions


class UnsupportedSchemaWithSelectException extends IllegalArgumentException {

  override def getMessage = {
    s"Applying schema and transformation in the same stage is not supported. For example '.schema(...).select(...)'"
  }

}
