package com.keks.sf.soap.v1

import com.keks.sf._
import com.keks.sf.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.mule.tools.soql.query.SOQLQuery

import java.sql.Timestamp


/**
  * Spark DataSource Api V1 implementation.
  * Supporting elimination of unneeded columns and filtration using selected predicates.
  * On Driver side:
  * 1) getting the 'offsetColumn' from configuration or taking default (Systemmodstamp).
  * 2) getting the 'initial offset' from conf or salesforce table.
  * 3) getting the 'last offset' from conf or salesforce table.
  * 4) Splitting query in partitions by 'loadNumPartitions' for example '3' and adding an 'order by' clause
  * '"select id, name from account where (name = 'alex') AND (age >=  0 AND age < 10) ORDER BY age"'
  * '"select id, name from account where (name = 'alex') AND (age >= 10 AND age < 20) ORDER BY age"'
  * '"select id, name from account where (name = 'alex') AND (age >= 20 AND age < 30) ORDER BY age"'
  * 5) executing each query on Executor in parallel.
  * Each executor keeps the last offset value from the previous soap batch to continue loading from this offset if an exception occurs.
  * This approach can produce duplicates but guarantee the data consistency.
  *
  * @param sfOptions        spark options wrapped in SfOptions
  * @param predefinedSchema .schema(...)
  * @param sparkSession     spark session
  */
case class SoapDataSourceBatchReaderV1(sfOptions: SfOptions,
                                       predefinedSchema: Option[StructType])(implicit sparkSession: SparkSession,
                                                                             queryId: UniqueQueryId,
                                                                             sfSoapConnection: SfSoapConnection,
                                                                             parsedSoqlData: ParsedSoqlData)
  extends BaseRelation
    with PrunedFilteredScan
    with DataSourceRegister
    with InsertableRelation
    with Serializable
    with LogSupport {

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
    * If true then RowEncoder(schema).resolveAndBind() should be used
    * and type erasure hack to pass RDD[InternalRow] back as RDD[Row] is impossible.
    */
  override val needConversion: Boolean = false

  /**
    * Sending partitions to executors.
    * Salesforce have some limitations with filtering Null vales,
    * so such filters are not pushed directly to salesforce
    * and will be processed by spark after loading.
    * Getting result spark schema.
    * Adding valid spark filters to SOQL query.
    *
    * @param requiredColumns spark selected columns.
    * @param filters         spark filters like .filter(...)
    * @return
    */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (_, notNullFilters) = filters.partition {
      case _: IsNotNull => true
      case _: IsNull => true
      case _ => false
    }
    val (rowSchema, resultSoql) = getRowSchemaAndSoql(requiredColumns, predefinedSchema)

    val soqlWithFilters = SfUtils.addFilters(resultSoql, notNullFilters)

    new SoapPartitionV1(
      soqlFromDriver = soqlWithFilters.toSOQLText,
      sfOptions,
      rowSchema,
      initialPartitions).asInstanceOf[RDD[Row]]
  }

  override def shortName = "salesforce-soap"

  override def sqlContext: SQLContext = sparkSession.sqlContext

  /**
    * If spark query contains .schema(...) then this schema is used
    * and columns that exist in defined schema but don't exist in
    * salesforce table will be null.
    *
    * @return
    */
  override def schema: StructType = {
    predefinedSchema.foreach(SfSparkSchemaUtils.comparePredefinedSchemaTypesWithTableSchemaTypes(_, parsedSoqlData.soqlSchema))
    predefinedSchema.getOrElse(parsedSoqlData.soqlSchema)
  }

  // TODO not implemented
  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???

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
