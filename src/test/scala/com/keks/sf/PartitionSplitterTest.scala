package com.keks.sf

import com.keks.sf.PartitionSplitter.rebuildBounds
import utils.TestBase


class PartitionSplitterTest extends TestBase {

  "PartitionSplitter#createPartitions" should "correctly split in 3 integer periods" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val res = PartitionSplitter.createBounds("1000", "2500", 3)
    val exp = Seq(
      ("1000", "1500"),
      ("1500", "2000"),
      ("2000", "2500"))
    res should contain theSameElementsInOrderAs exp
  }

  "PartitionSplitter#createPartitions" should "correctly split in 1 integer periods" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val res = PartitionSplitter.createBounds("1000", "2500", 1)
    val exp = Seq(
      ("1000", "2500"))

    res should contain theSameElementsInOrderAs exp
  }

  "PartitionSplitter#createPartitions" should "fail if lowerBound > upperBound in integer" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    assertThrows[IllegalArgumentException] {
      PartitionSplitter.createBounds(
        "5000",
        "2500",
        1)
    }
  }

  "PartitionSplitter#createPartitions" should "fail if incorrect integer format" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    assertThrows[IllegalArgumentException] {
      PartitionSplitter.createBounds(
        "5000.0",
        "2500.0",
        1)
    }
  }

  "PartitionSplitter#createPartitions" should "correctly split in 3 double periods" in {
    import com.keks.sf.PartitionTypeOperationsIml.doubleOperations
    val res = PartitionSplitter.createBounds(
      "1000.0",
      "2500.0",
      3)

    val exp = Seq(
      ("1000.0", "1500.0"),
      ("1500.0", "2000.0"),
      ("2000.0", "2500.0"))

    res should contain theSameElementsInOrderAs exp
  }

  "PartitionSplitter#createPartitions" should "correctly split in 1 double periods" in {
    import com.keks.sf.PartitionTypeOperationsIml.doubleOperations
    val res = PartitionSplitter.createBounds(
      "1000.0",
      "2500.0",
      1)

    val exp = Seq(
      ("1000.0", "2500.0"))

    res should contain theSameElementsInOrderAs exp
  }

  "PartitionSplitter#createPartitions" should "fail if lowerBound > upperBound in double" in {
    import com.keks.sf.PartitionTypeOperationsIml.doubleOperations
    assertThrows[IllegalArgumentException] {
      PartitionSplitter.createBounds(
        "5000.0",
        "2500.0",
        1)
    }
  }

  "PartitionSplitter#createPartitions" should "fail if incorrect double format" in {
    import com.keks.sf.PartitionTypeOperationsIml.doubleOperations
    assertThrows[IllegalArgumentException] {
      PartitionSplitter.createBounds(
        "abcd",
        "abcd",
        1)
    }
  }

  "PartitionSplitter#createPartitions" should "correctly split in 3 date time periods" in {
    import com.keks.sf.PartitionTypeOperationsIml.timeOperations
    val res = PartitionSplitter.createBounds(
      "2019-01-01T00:00:00.000Z",
      "2020-01-01T00:00:00.000Z",
      3)
    val exp = Seq(
      ("2019-01-01T00:00:00.000Z", "2019-05-02T16:00:00.000Z"),
      ("2019-05-02T16:00:00.000Z", "2019-09-01T08:00:00.000Z"),
      ("2019-09-01T08:00:00.000Z", "2020-01-01T00:00:00.000Z"))

    res should contain theSameElementsInOrderAs exp
  }

  "PartitionSplitter#createPartitions" should "correctly split in 1 date time period" in {
    import com.keks.sf.PartitionTypeOperationsIml.timeOperations
    val res = PartitionSplitter.createBounds(
      "2019-01-01T00:00:00.000Z",
      "2020-01-01T00:00:00.000Z",
      1)

    val exp = Seq(
      ("2019-01-01T00:00:00.000Z", "2020-01-01T00:00:00.000Z"))

    res should contain theSameElementsInOrderAs exp
  }

  "PartitionSplitter#createPartitions" should "fail if lowerBound > upperBound in date time" in {
    import com.keks.sf.PartitionTypeOperationsIml.timeOperations
    assertThrows[IllegalArgumentException] {
      PartitionSplitter.createBounds(
        "2020-01-01T00:00:00.000Z",
        "2019-01-01T00:00:00.000Z",
        3)
    }
  }

  "PartitionSplitter#createPartitions" should "fail if incorrect date time format" in {
    import com.keks.sf.PartitionTypeOperationsIml.timeOperations
    assertThrows[IllegalArgumentException] {
      PartitionSplitter.createBounds(
        "2020-01-01T00:00:00.000Z",
        "2019-01-01T00:00:00.000Z",
        3)
    }
  }

  "PartitionSplitter#rebuildBounds" should "change bounds from 2 to 3" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val oldBounds =
      Array(
        ("1", "10"),
        ("10", "20")
        )
    val res = rebuildBounds(oldBounds, 3)
    assert(res.length == 3)
  }

  "PartitionSplitter#rebuildBounds" should "change bounds from 2 to 4" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val oldBounds =
      Array(
        ("1", "10"),
        ("10", "20")
        )
    val res = rebuildBounds(oldBounds, 4)
    assert(res.length == 4)
  }

  "PartitionSplitter#rebuildBounds" should "change bounds from 2 to 5" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val oldBounds =
      Array(
        ("1", "10"),
        ("10", "20")
        )
    val res = rebuildBounds(oldBounds, 5)
    assert(res.length == 5)
  }

  "PartitionSplitter#rebuildBounds" should "change bounds from 3 to 4" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val oldBounds =
      Array(
        ("1", "10"),
        ("10", "20"),
        ("20", "30")
        )

    val res = rebuildBounds(oldBounds, 4)
    assert(res.length == 4)
  }

  "PartitionSplitter#rebuildBounds" should "change bounds from 3 to 5" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val oldBounds =
      Array(
        ("1", "10"),
        ("10", "20"),
        ("20", "30")
        )

    val res = rebuildBounds(oldBounds, 5)
    assert(res.length == 5)
  }

  "PartitionSplitter#rebuildBounds" should "change bounds from 1 to 4" in {
    import com.keks.sf.PartitionTypeOperationsIml.intOperations
    val oldBounds =
      Array(
        ("1", "10"),
        ("10", "20"),
        ("20", "30")
        )

    val res = rebuildBounds(oldBounds, 4)
    assert(res.length == 4)
  }

}
