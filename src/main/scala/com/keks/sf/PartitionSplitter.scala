package com.keks.sf

import com.keks.sf.enums.PartitionColType.{DOUBLE, INTEGER, ISO_DATE_TIME, STRING}
import com.keks.sf.implicits.RichTry
import com.keks.sf.soap.{ExecutorMetrics, SfSparkPartition}
import com.keks.sf.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.joda.time.DateTime

import java.sql.Timestamp
import scala.math.Ordering
import scala.util.Try


object PartitionSplitter extends LogSupport {

  implicit class OperationSyntax[T](left: T)(implicit enc: PartitionTypeOperations[T]) {

    def + (right: T) = enc.add(left, right)

    def - (right: T) = enc.sub(left, right)

    def / (value: Int) = enc.div(left, value)

    def * (num: Int) = enc.mul(left, num)

    def print: String = enc.print(left)

  }

  def createBounds[T](lowerBoundStr: String,
                      upperBoundStr: String,
                      numPartitions: Int)(implicit enc: PartitionTypeOperations[T]): Array[(String, String)] = {
    val lowerBound = enc.parse(lowerBoundStr)
    val upperBound = enc.parse(upperBoundStr)
    enc.checkBounds(lowerBound, upperBound)
    val partStep = (upperBound - lowerBound) / numPartitions
    (1 to numPartitions).toArray.map {
      case 1 => (lowerBound, lowerBound + partStep)
      case partNum if partNum == numPartitions => (lowerBound + (partStep * (partNum - 1)), upperBound)
      case partNum => (lowerBound + (partStep * (partNum - 1)), lowerBound + (partStep * partNum))
    }.map { case (x, y) => (x.print, y.print) }.distinct
  }

  def rebuildBounds[T](bounds: Array[(String, String)],
                       numPartitions: Int)
                      (implicit enc: PartitionTypeOperations[T]): Array[(String, String)] = {
    def rec(recBounds: Array[(String, String)], count: Int): Array[(String, String)] = {
      if (count == 0) {
        recBounds
      } else {
        val (lowerStr, upperStr) = recBounds.head
        if (recBounds.length == 1 && count != 1) {
          val x = createBounds(lowerStr, upperStr, count + 1)
          x
        } else {
          val y =  createBounds(lowerStr, upperStr, 2)
          y ++ rec(recBounds.tail, count - 1)
        }
      }
    }

    val needToAdd = numPartitions - bounds.length
    rec(bounds, needToAdd)
  }

  // TODO while processing partitions in streaming when no shuffling bounds, then last offset is moved as first but condition is still >= and duplicates occurs
  def generateSfSparkPartitions[T](bounds: Array[(String, String)],
                                   partitionCol: String,
                                   isFirstPartitionCondOperatorGreaterAndEqual: Boolean)
                                  (implicit enc: PartitionTypeOperations[T]): Array[SfSparkPartition] = {
    val getSfPartitionFunc =
      (left: String, right: String, leftOperator: String, rightOperator: String, id: Int) =>
        SfSparkPartition(
          id = id,
          column = partitionCol,
          lowerBound = enc.parseToSfValue(left),
          upperBound = enc.parseToSfValue(right),
          leftCondOperator = leftOperator,
          rightCondOperator = rightOperator,
          enc.operationTypeStr == classOf[String].getName,
          ExecutorMetrics(offset = left))
    val numPartitions = bounds.length
    bounds.zipWithIndex
      .map {
        case ((left, right), i) if i == 0  && numPartitions == 1=>
          val leftOperator: String = if (isFirstPartitionCondOperatorGreaterAndEqual) ">=" else ">"
          getSfPartitionFunc(left, right, leftOperator, "<=", i)
        case ((left, right), i) if i == 0  && numPartitions != 1=>
          val leftOperator: String = if (isFirstPartitionCondOperatorGreaterAndEqual) ">=" else ">"
          getSfPartitionFunc(left, right, leftOperator, "<", i)
        case ((left, right), i) if i == numPartitions - 1 =>
          getSfPartitionFunc(left, right, ">=", "<=", i)
        case ((left, right), i) =>
          getSfPartitionFunc(left, right, ">=", "<", i)
      }
  }

  // TODO test it
  def recreateSfSparkPartitions[T](partitions: Array[SfSparkPartition],
                                   numPartitions: Int,
                                   offsetColName: String)
                                  (implicit enc: PartitionTypeOperations[T]): Array[SfSparkPartition] = {
    val notFinishedPartitions = partitions
      .filterNot(_.executorMetrics.isDone)
      .map { partition =>
        val lastOffsetFromExecutor = enc.parseToSfValue(partition.executorMetrics.offset)
        partition.copy(lowerBound = lastOffsetFromExecutor)
      }
    info(s"Driver. Finished '${numPartitions - notFinishedPartitions.length}' of $numPartitions")
    if (notFinishedPartitions.length >= numPartitions) {
      info(s"Recreated partitions without shuffling bounds")
      notFinishedPartitions
    } else if (notFinishedPartitions.length == 0) {
      Array.empty
    } else {
      info(s"Recreating partitions with shuffling bounds")
      val bounds = notFinishedPartitions.map(e => (e.lowerBound, e.upperBound))
      val newBounds = PartitionSplitter.rebuildBounds(bounds, numPartitions)
      generateSfSparkPartitions(newBounds, offsetColName, isFirstPartitionCondOperatorGreaterAndEqual = true)
    }
  }

  def getOperationType(partitionColType: DataType) = {
    partitionColType match {
      case TimestampType => PartitionTypeOperationsIml.timeOperations
      case IntegerType => PartitionTypeOperationsIml.intOperations
      case DoubleType => PartitionTypeOperationsIml.doubleOperations
      case StringType => PartitionTypeOperationsIml.stringOperations
      case _ => throw new IllegalArgumentException(
        s"Cannot map defined column type '$partitionColType' with exist: '$TimestampType, $IntegerType, $DoubleType, $StringType'")
    }
  }

}

object PartitionTypeOperationsIml extends LogSupport {

  implicit val timeOperations: PartitionTypeOperations[Timestamp] = new PartitionTypeOperations[Timestamp] {

    override val operationTypeStr = classOf[Timestamp].getName

    private val millisToIso: Long => DateTime = (millis: Long) => DateTimeUtils.parseSqlTimestampAsStringToDate(new Timestamp(millis).toString)

    override def checkBounds(lowerBound: Timestamp, upperBound: Timestamp): Unit =
      require(upperBound.getTime > lowerBound.getTime, s"upperBound: '$upperBound' should be greater then lowerBound: '$lowerBound'")

    override def add(left: Timestamp, right: Timestamp) = new Timestamp(left.getTime + right.getTime)

    override def sub(left: Timestamp, right: Timestamp) = new Timestamp(left.getTime - right.getTime)

    override def div(value: Timestamp, num: Int) = new Timestamp(value.getTime / num)

    override def mul(value: Timestamp, num: Int) = new Timestamp(value.getTime * num)

    override def parse(value: String) = {
      Try(DateTimeUtils.isoStrToTimestamp(value))
        .onFailure(ex => throw new IllegalArgumentException(s"Cannot parse '$value' to DateTime while building $ISO_DATE_TIME partition", ex))
    }

    override def print(value: Timestamp) = millisToIso(value.getTime).toString

    override def compareType(left: Timestamp, right: Timestamp): Int = if (left.getTime < right.getTime) -1 else if (left.getTime > right.getTime) 1 else 0

    override val ordering = new Ordering[Timestamp] {
      override def compare(x: Timestamp, y: Timestamp) = compareType(x, y)
    }

    override def parseToSfValue(value: String) = s"$value"
  }

  implicit val intOperations: PartitionTypeOperations[Int] = new PartitionTypeOperations[Int] {
    override val operationTypeStr = classOf[Int].getName

    override def checkBounds(lowerBound: Int, upperBound: Int): Unit =
      require(upperBound > lowerBound, s"upperBound: '$upperBound' should be greater then lowerBound: '$lowerBound'")

    override def add(left: Int, right: Int) = left + right

    override def sub(left: Int, right: Int) = left - right

    override def div(value: Int, num: Int) = value / num

    override def mul(value: Int, num: Int) = value * num

    override def parse(value: String) = {
      Try(value.toInt)
        .onFailure(ex => throw new IllegalArgumentException(s"Cannot parse '$value' to Integer while building $INTEGER partition", ex))
    }

    override def compareType(left: Int, right: Int): Int = if (left < right) -1 else if (left > right) 1 else 0

    override val ordering = new Ordering[Int] {
      override def compare(x: Int, y: Int) = compareType(x, y)
    }

    override def parseToSfValue(value: String) = s"$value"
  }

  implicit val doubleOperations: PartitionTypeOperations[Double] = new PartitionTypeOperations[Double] {
    override val operationTypeStr = classOf[Double].getName

    override def checkBounds(lowerBound: Double, upperBound: Double): Unit =
      require(upperBound > lowerBound, s"upperBound: '$upperBound' should be greater then lowerBound: '$lowerBound'")

    override def add(left: Double, right: Double) = left + right

    override def sub(left: Double, right: Double) = left - right

    override def div(value: Double, num: Int) = value / num

    override def mul(value: Double, num: Int) = value * num

    override def parse(value: String) = {
      Try(value.toDouble)
        .onFailure(ex => throw new IllegalArgumentException(s"Cannot parse '$value' to Double while building $DOUBLE partition", ex))
    }

    override def compareType(left: Double, right: Double): Int = if (left < right) -1 else if (left > right) 1 else 0

    override val ordering = new Ordering[Double] {
      override def compare(x: Double, y: Double) = compareType(x, y)
    }

    override def parseToSfValue(value: String) = s"$value"
  }

  implicit val stringOperations: PartitionTypeOperations[String] = new PartitionTypeOperations[String] {
    private val errorMessage = s"Partitioning doesn't support $STRING. Only one partition can be used."
    override val operationTypeStr = classOf[String].getName

    override def checkBounds(lowerBound: String, upperBound: String): Unit = throw new IllegalArgumentException(errorMessage)

    override def add(left: String, right: String) = throw new IllegalArgumentException(errorMessage)

    override def sub(left: String, right: String) = throw new IllegalArgumentException(errorMessage)

    override def div(value: String, num: Int) = throw new IllegalArgumentException(errorMessage)

    override def mul(value: String, num: Int) = throw new IllegalArgumentException(errorMessage)

    override def parse(value: String): String = throw new IllegalArgumentException(errorMessage)

    override def compareType(left: String, right: String): Int = throw new IllegalArgumentException(errorMessage)

    override val ordering = new Ordering[String] {
      override def compare(x: String, y: String) =
        throw new IllegalArgumentException(errorMessage)
    }

    override def parseToSfValue(value: String) = if (value.head == ''' && value.last == ''') value else s"'$value'"
  }

}

trait PartitionTypeOperations[T] {

  val operationTypeStr: String

  val ordering: Ordering[T]

  def checkBounds(lowerBound: T, upperBound: T)

  def add(left: T, right: T): T

  def sub(left: T, right: T): T

  def div(value: T, num: Int): T

  def mul(value: T, num: Int): T

  def parse(value: String): T

  def print(value: T): String = value.toString

  def compareType(left: T, right: T): Int

  def parseToSfValue(value: String): String

}
