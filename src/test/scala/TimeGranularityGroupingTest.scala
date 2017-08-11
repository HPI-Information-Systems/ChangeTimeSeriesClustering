import java.time.LocalDateTime

import main.{ChangeRecord, TimeGranularityGrouping}
import org.scalatest._

class TimeGranularityGroupingTest extends FlatSpec {

  "Daily Aggregation" should "correctly aggregate by day of year" in {
    val timestamp1 = LocalDateTime.of(2000,10,10,12,0)
    val timestamp2 = LocalDateTime.of(2000,10,10,13,0)
    val timestamp3 = LocalDateTime.of(2000,10,11,12,0)
    val timestamp4 = LocalDateTime.of(2001,10,10,12,0)
    val records = List(new ChangeRecord("a","some-prop","some-value",timestamp1),
      new ChangeRecord("a","some-prop2","some-value",timestamp2),
      new ChangeRecord("a","some-prop3","some-value",timestamp3),
      new ChangeRecord("a","some-prop4","some-value",timestamp4)
    )
    val groupingObject = new TimeGranularityGrouping(2000,2001)
    val ts = groupingObject.toSingleDimensionalDailyTimeSeries("a",records.iterator)
    assert(ts.numNonZeroYValues == 3)
    val yValues = ts.getDim(ts.dimNames(0) )
    assert(yValues.sum == 4)
    assert(yValues.size == timestamp1.toLocalDate.lengthOfYear() + timestamp4.toLocalDate.lengthOfYear())
    //check that the actual values are correct:
    val expected2Index = timestamp1.getDayOfYear -1
    val expected1Indices = List(timestamp3.getDayOfYear -1,timestamp4.getDayOfYear-1+timestamp1.toLocalDate.lengthOfYear())
    assert(yValues(expected2Index)==2)
    assert(expected1Indices.map(yValues) == List(1.0,1.0))
    //check that the correct timestamps are at that position:
    val dateFor2 = timestamp1.toLocalDate
    assert(ts.timeAxis(expected2Index).toLocalDateTime.toLocalDate == dateFor2)
  }

  "Monthly Aggregation" should "correctly aggregate by month of year" in {
    val timestamp1 = LocalDateTime.of(2000,10,10,12,0)
    val timestamp2 = LocalDateTime.of(2000,10,10,13,0)
    val timestamp3 = LocalDateTime.of(2000,10,11,12,0)
    val timestamp4 = LocalDateTime.of(2000,11,10,12,0)
    val timestamp5 = LocalDateTime.of(2001,11,10,12,0)
    val records = List(new ChangeRecord("a","some-prop","some-value",timestamp1),
      new ChangeRecord("a","some-prop2","some-value",timestamp2),
      new ChangeRecord("a","some-prop3","some-value",timestamp3),
      new ChangeRecord("a","some-prop4","some-value",timestamp4),
      new ChangeRecord("a","some-prop5","some-value",timestamp5)
    )
    val groupingObject = new TimeGranularityGrouping(2000,2001)
    val ts = groupingObject.toSingleDimensionalMonthlyTimeSeries("a",records.iterator)
    assert( 3 == ts.numNonZeroYValues)
    val yValues = ts.getDim(ts.dimNames(0) )
    assert(5 == yValues.sum)
    assert(24 == yValues.size)
    val expected3Index = 10-1
    val expected1Indices = List(11-1,22)
    assert(3 == yValues(expected3Index))
    assert(List(1.0,1.0) == expected1Indices.map(yValues))
    //check that the correct timestamps are at that position:
    val dateFor3 = groupingObject.toStandardDateInMonth(2000,10).toLocalDate
    assert(ts.timeAxis(expected3Index).toLocalDateTime.toLocalDate == dateFor3)
  }

  "Yearly Aggregation" should "correctly aggregate by year" in {
    val timestamp1 = LocalDateTime.of(2000,1,10,12,0)
    val timestamp2 = LocalDateTime.of(2000,6,10,13,0)
    val timestamp3 = LocalDateTime.of(2000,12,11,12,0)
    val timestamp4 = LocalDateTime.of(2001,11,10,12,0)
    val timestamp5 = LocalDateTime.of(2002,11,10,12,0)
    val records = List(new ChangeRecord("a","some-prop","some-value",timestamp1),
      new ChangeRecord("a","some-prop2","some-value",timestamp2),
      new ChangeRecord("a","some-prop3","some-value",timestamp3),
      new ChangeRecord("a","some-prop4","some-value",timestamp4),
      new ChangeRecord("a","some-prop5","some-value",timestamp5)
    )
    val groupingObject = new TimeGranularityGrouping(2000,2002)
    val ts = groupingObject.toSingleDimensionalYearlyTimeSeries("a",records.iterator)
    assert( 3 == ts.numNonZeroYValues)
    val yValues = ts.getDim(ts.dimNames(0) )
    assert(5 == yValues.sum)
    assert(3 == yValues.size)
    val expected3Index = 0
    val expected1Indices = List(1,2)
    assert(3 == yValues(expected3Index))
    assert(List(1.0,1.0) == expected1Indices.map(yValues))
    //check that the correct timestamps are at that position:
    val dateFor3 = groupingObject.toStandardDateInYear(2000).toLocalDate
    assert(ts.timeAxis(expected3Index).toLocalDateTime.toLocalDate == dateFor3)
  }
}