package com.keks.spark.sf.exceptions

class NoSuchSalesforceFieldException(tableName: String,
                                     field: String,
                                     sfFieldsOpt: Option[Array[String]] = None,
                                     colDescriptionOpt: Option[String] = None) extends IllegalArgumentException {

  override def getMessage = {
    s"Salesforce table '$tableName' doesn't contain ${colDescriptionOpt.map(e => s"'$e'").getOrElse("")} col: $field." +
      sfFieldsOpt.map(sfFields => s"\nAvailable fields are: ${sfFields.mkString(",")}").getOrElse("")
  }

}
