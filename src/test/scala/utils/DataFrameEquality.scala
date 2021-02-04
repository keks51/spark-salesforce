package utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{LongType, StructField, StructType}


trait DataFrameEquality {
  
  private val DF_NAME_COLUMN = "test_df"
  private val INDEX_COLUMN = "test_index_column"
  private val EXPECTED = "expected"
  private val RESULT = "result"

  def assertSqlColumnsEqual(expectedCol: Column, resultCol: Column): Unit = {
    assert(expectedCol.toString == resultCol.toString)
  }
  
  def assertDatasetsEqual[T](expectedDS: Dataset[T],
                             resultDS: Dataset[T]): Unit = {
    assertDataFramesEqual(expectedDS.toDF(), resultDS.toDF())
  }
  
  def assertDatasetEqualDataFrame[T](expectedDS: Dataset[T],
                                     resultDF: DataFrame): Unit = {
    assertDataFramesEqual(expectedDS.toDF(), resultDF)
  }
  
  def assertDataFramesEqual(expectedDF: DataFrame,
                            resultDF: DataFrame): Unit = {
    val expColumns = expectedDF.dtypes
    val resColumns = resultDF.dtypes
    assertNoDuplicateColumns(resColumns)
    assertSchemaEqual(expColumns, resColumns)
    assertDfDataEqual(expectedDF.select(expColumns.map(c => col(c._1)):_*), resultDF.select(expColumns.map(c => col(c._1)):_*), expColumns.map(_._1))
  }
  
  private def assertNoDuplicateColumns(resColumns: Array[(String, String)]): Unit = {
    val duplicatesList = resColumns.map(_._1).diff(resColumns.map(_._1).distinct)
    assert(
      duplicatesList.isEmpty,
      s"Result DataFrame contains duplicate columns [${duplicatesList.mkString(", ")}]")
  }
  
  private def assertSchemaEqual(expColumns: Array[(String, String)],
                                resColumns: Array[(String, String)]): Unit = {
    val expDiffResColNames = expColumns.map(_._1).diff(resColumns.map(_._1))
    assert(expDiffResColNames.isEmpty,
      s"Expected columns contains [${expDiffResColNames.mkString(", ")}], but result doesn't contain them")
    val resDiffExpColNames = resColumns.map(_._1).diff(expColumns.map(_._1))
    assert(resDiffExpColNames.isEmpty,
      s"Result columns contains [${resDiffExpColNames.mkString(", ")}], but expected doesn't contain them")

    val expDiffResColTypes = resColumns.diff(expColumns)
    assert(expDiffResColTypes.isEmpty,
      s"Expected columns contains [${expDiffResColTypes.mkString(", ")}], but result columns have different types " +
        s"[${expColumns.diff(resColumns).mkString(", ")}]")
  }
  
  private def assertDfDataEqual(expectedDF: DataFrame, resultDF: DataFrame, columns: Seq[String]): Unit = {
    val expected = expectedDF.sort(columns.map(col): _*).collect.toSeq
    val result = resultDF.sort(columns.map(col): _*).collect.toSeq
    
    val expectedSize = expected.size
    val resultSize = result.size
    assert(expectedSize == resultSize, s"Number of rows not equals. ExpectedSize=$expectedSize. ResultSize=$resultSize")
    expected.zipWithIndex.foreach { case (expRow, rowIndex) =>
      columns.foreach { colName =>
        val excColValue = expRow.getAs[Any](colName)
        val resColValue = result(rowIndex).getAs[Any](colName)
        if (excColValue != resColValue) printDiff(expectedDF, resultDF, columns)
      }
    }
  }
  
  private def printDiff(expected: DataFrame, result: DataFrame, columns: Seq[String]): Unit = {
    val expectedWithIndex = dfZipWithIndex(expected.sort(columns.map(col): _*), INDEX_COLUMN).cache
    val resultWithIndex = dfZipWithIndex(result.sort(columns.map(col): _*), INDEX_COLUMN).cache
    println("Expected:")
    expectedWithIndex.show(false)
    println("Result:")
    resultWithIndex.show(false)
    
    val expectedDiff = expectedWithIndex
      .except(resultWithIndex).sort(columns.map(col): _*).withColumn(DF_NAME_COLUMN, lit(EXPECTED))
    val resultDiff = resultWithIndex
      .except(expectedWithIndex).sort(columns.map(col): _*).withColumn(DF_NAME_COLUMN, lit(RESULT))
    val diff = expectedDiff
      .union(resultDiff)
      .sort(INDEX_COLUMN, DF_NAME_COLUMN)
      .select(DF_NAME_COLUMN, "*")
      .cache
    println("Expected Table doesn't equal Result Table")
    diff.show(false)
    
    // compare two rows with the same INDEX_COLUMN value
    diff
      .collect
      .sortBy(e => e.getAs[Long](INDEX_COLUMN))
      .sliding(2,2)
      .toList
      .foreach { case Array(expRow, resRow) =>
        println(s"$INDEX_COLUMN = ${expRow.getAs[String](INDEX_COLUMN)}")
        columns.zipWithIndex.foreach { case (colName, colIndex) =>
          val excColValue = expRow.getAs[Any](colName)
          val resColValue = resRow.getAs[Any](colName)
          if (excColValue != resColValue)
            println(s"Column: ${columns(colIndex)} => expected: '$excColValue' but found: '$resColValue'")
        }
      }
    
    expectedWithIndex.unpersist
    resultWithIndex.unpersist
    diff.unpersist
    throw new java.lang.AssertionError("Tables are not equal")
  }
  
  /**
    * Input:
    * +---------+
    * |user_id  |
    * +---------+
    * |User ID 1|
    * |User ID 4|
    * |User ID 3|
    * +---------+
    * Output:
    * +---------------+---------+
    * |indexColumnName|user_id  |
    * +---------------+---------+
    * |1              |User ID 1|
    * |2              |User ID 4|
    * |3              |User ID 3|
    * +---------------+---------+
    */
  private def dfZipWithIndex(df: DataFrame, indexColumnName: String): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map { case(row, index) => Row.fromSeq(Seq(index + 1) ++ row.toSeq) },
      StructType(Array(StructField(indexColumnName, LongType, nullable = false)) ++ df.schema.fields))
  }
  
}
