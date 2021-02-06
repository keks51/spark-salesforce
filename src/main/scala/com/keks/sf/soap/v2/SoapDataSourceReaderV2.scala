package com.keks.sf.soap.v2

import com.keks.sf.util._
import com.keks.sf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{Filter, IsNotNull, IsNull}
import org.apache.spark.sql.types.StructType
import org.mule.tools.soql.query.SOQLQuery

import java.sql.Timestamp


/**
  * SoapDataSourceRead provider.
  * Supporting elimination of unneeded columns and filtration using selected predicates.
  *
  * @param sfOptions        spark options wrapped in SfOptions
  * @param predefinedSchema .schema(...)
  * @param spark            spark session
  */
abstract class SoapDataSourceReaderV2(sfOptions: SfOptions,
                                      predefinedSchema: Option[StructType])(implicit spark: SparkSession,
                                                                            queryId: UniqueQueryId,
                                                                            sfSoapConnection: SfSoapConnection,
                                                                            parsedSoqlData: ParsedSoqlData)
//  extends SfTableDescription
  extends DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with LogSupport {

  val hadoopConf = spark.sessionState.newHadoopConf
  val serializableHadoopConf: SerializableConfiguration = new SerializableConfiguration(hadoopConf)

  val isSelectAll = parsedSoqlData.isSelectAll
  val offsetColName = parsedSoqlData.offsetColName
  val sfTableName = parsedSoqlData.sfTableName

  private var notNullFiltersArr = Array.empty[Filter]
  private var requiredColumns = Array.empty[String]
  private var schema: StructType = {
    predefinedSchema.foreach(SfSparkSchemaUtils.comparePredefinedSchemaTypesWithTableSchemaTypes(_, parsedSoqlData.soqlSchema))
    predefinedSchema.getOrElse(parsedSoqlData.soqlSchema)
  }

  /* Spark result schema and SOQL without push down filters */
  lazy val (rowSchema, resultSoql) = getRowSchemaAndSoql(requiredColumns, predefinedSchema)
  /* Soql with push down filters */
  lazy val soqlToQuery: String = SfUtils.addFilters(resultSoql, notNullFiltersArr).toSOQLText

  /* Initial spark schema */
  override def readSchema(): StructType = this.schema

  /* Pruning columns directly to SOQL query */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.schema = requiredSchema
    requiredColumns = requiredSchema.fields.map(_.name)
  }

  /**
    * Salesforce have some limitations with filtering Null vales,
    * so such filters are not pushed directly to salesforce
    * and will be processed by spark after loading.
    *
    * @param filters like .filter(...)
    * @return filters that should be processed by spark
    */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (nullFilters, notNullFilters) = filters.partition {
      case _: IsNotNull => true
      case _: IsNull => true
      case _ => false
    }
    notNullFiltersArr = notNullFilters
    notNullFiltersArr ++ nullFilters
  }

  /**
    * Filters that should be added to SOQL query.
    */
  override def pushedFilters(): Array[Filter] = {
    notNullFiltersArr
  }


  /* Getting initial Start offset value from conf or querying Salesforce Table */
  lazy val initialStartOffset = sfOptions
    .initialOffsetOpt
    .map { offset => info(s"Taking initial offset from configuration: [$offset]."); offset }
    .orElse {
      sfSoapConnection.getFirstOffsetFromSF(parsedSoqlData.offsetColName)
    }

  /* Getting initial End offset value from conf or querying Salesforce Table */
  lazy val initialEndOffset = sfOptions
    .sfEndOffsetOpt
    .map { offset => info(s"Taking end offset from configuration: [$offset]."); offset }
    .orElse {
      sfSoapConnection.getLatestOffsetFromSF(parsedSoqlData.offsetColName)
    }

  /**
    * Getting TypeOperations implementation for offset col type.
    * If offset col type is Int then PartitionTypeOperationsIml.intOperations is used.
    */
  implicit val partitionColType: PartitionTypeOperations[_ >: Timestamp with Int with Double with String] = parsedSoqlData.partitionColType

  /* Getting initial spark partitions that should be processed on executors */
  lazy val initialPartitions = SfUtils
    .getSparkPartitions(parsedSoqlData.offsetColName,
                        parsedSoqlData.sfTableName,
                        initialStartOffset,
                        initialEndOffset,
                        sfOptions.sfLoadNumPartitions,
                        isFirstPartitionCondOperatorGreaterAndEqual = true)

  /**
    * Getting Struct Schema and modifying soql depending on
    * columns selected by spark like .select(...) and user defined schema by .schema(...)
    * Adding order by clause and offset col to select case
    *
    * @param sparkSelectedCols like .select(...)
    * @param predefinedSchema  .schema(...)
    * @return (schema to create spark row, SOQl to get data from Salesforce table)
    */
  private def getRowSchemaAndSoql(sparkSelectedCols: Array[String], predefinedSchema: Option[StructType]) = {
    val (createdSchema, createdSoql) = SfSparkSchemaUtils.createSchemaAndModifySoql(
      sparkSelectedCols = sparkSelectedCols,
      soql = parsedSoqlData.soql,
      sfTableName = parsedSoqlData.sfTableName,
      sfTableColNames = parsedSoqlData.sfTableColNames,
      soqlSchema = parsedSoqlData.soqlSchema,
      predefinedSchema = predefinedSchema)

    val withOffsetSelect = addOffsetToSelect(createdSoql)

    (createdSchema, withOffsetSelect)
  }

  /* Adding offset col to select case */
  private def addOffsetToSelect(soql: SOQLQuery) = {
    val selectCols = SoqlUtils.getSoqlSelectFieldNames(soql)
    if (selectCols.map(_.toLowerCase).contains(parsedSoqlData.offsetColName.toLowerCase)) {
      soql
    } else {
      SoqlUtils.fillSelectAll(soql, selectCols :+ parsedSoqlData.offsetColName)
    }
  }


}
