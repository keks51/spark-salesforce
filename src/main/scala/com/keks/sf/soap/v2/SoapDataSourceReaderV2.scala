package com.keks.sf.soap.v2

import com.keks.sf.soap.SfTableDescription
import com.keks.sf.util._
import com.keks.sf.{LogSupport, SerializableConfiguration, SfOptions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{Filter, IsNotNull, IsNull}
import org.apache.spark.sql.types.StructType


abstract class SoapDataSourceReaderV2(sfOptions: SfOptions,
                                      predefinedSchema: Option[StructType])(implicit spark: SparkSession)
  extends SfTableDescription
    with DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with LogSupport {

  val hadoopConf = spark.sessionState.newHadoopConf
  val serializableHadoopConf: SerializableConfiguration = new SerializableConfiguration(hadoopConf)

  override def getSfOptions = sfOptions

  private var filtersArr = Array.empty[Filter]
  private var requiredColumns = Array.empty[String]
  private var schema: StructType = {
    predefinedSchema.foreach(SfSparkSchemaUtils.comparePredefinedSchemaWithTableSchema(_, sfTableAsSparkStruct))
    predefinedSchema.getOrElse(sfTableAsSparkStruct)
  }

  lazy val (resultSchema, resultSoql) = getSchemaAndSoql(requiredColumns, predefinedSchema)
  lazy val soqlWithDefaultFilters: String = SfUtils.addFilters(resultSoql, filtersArr).toSOQLText

  override def readSchema(): StructType = this.schema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.schema = requiredSchema
    requiredColumns = requiredSchema.fields.map(_.name)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val(nullFilters, notNullFilters) = filters.partition {
      case _: IsNotNull => true
      case _: IsNull => true
      case _ => false
    }
    filtersArr = notNullFilters
    filtersArr ++ nullFilters
  }


  override def pushedFilters(): Array[Filter] = {
    filtersArr
  }

}
