# Problem Statement
```
let's suppose we have two pipeline one is filebased and second is streaming. 
We want to make sure new pipeline has no any missing data.
that is both pipeline should have same data count on daily basis.
streaming pipeline may have a more data than filebased.
We need daily count analysis for all tables.
```

# Solution

1. Retrieve current day and previous day.
```
import org.apache.spark.sql.catalyst.expressions.CurrentDate
import java.text.SimpleDateFormat
import java.util.Calendar
val sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
val date2 = sdf.format(Calendar.getInstance().getTime)
val cal = Calendar.getInstance
cal.setTime(sdf.parse(date2))
cal.add(Calendar.DAY_OF_YEAR, -1)
val previousDate = cal.getTime
val date1 = sdf.format(previousDate)
print(date1,date2)
```
- output: ```(2020-04-08 00:00:00,2020-04-09 00:00:00)```

2. Retreive Partitions.
```
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

def generateCondition(date: LocalDate): String = {
    val condition = new StringBuilder
    condition.
      append("(").
      append("year=").
      append(date.getYear).
      append(" ").append("and").append(" ").
      append("month=").
      append(date.getMonthValue).
      append(" ").append("and").append(" ").
      append("day=").
      append(date.getDayOfMonth).
      append(")").toString()
}

val currentLocalDate = LocalDate.parse(new SimpleDateFormat("yyyy-MM-dd").format(new Date()), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
var startDate = currentLocalDate.minusDays(5)
var endDate = currentLocalDate.plusDays(5)

var dayRangeList = new scala.collection.mutable.ListBuffer[LocalDate]()
while (!startDate.isAfter(endDate)) {
    dayRangeList += startDate
    startDate=startDate.plusDays(1)
}
print(dayRangeList)

var intermediateRangeQuery = for (elem <- dayRangeList) yield {
    val dateRangeFilter = new StringBuilder
    dateRangeFilter.append(generateCondition(elem)).append(" ").append("or").append(" ").toString()
}
intermediateRangeQuery += " 1=2 "

val finalRangeQuery = new StringBuilder
finalRangeQuery.append(" ( ")
intermediateRangeQuery.foreach(elem => finalRangeQuery.append(elem))
finalRangeQuery.append(" ) ")
finalRangeQuery.toString()

print(finalRangeQuery)
```
- output : 
```
( (year=2020 and month=4 and day=4) or (year=2020 and month=4 and day=5) or (year=2020 and month=4 and day=6) or (year=2020 and month=4 and day=7) or (year=2020 and month=4 and day=8) or (year=2020 and month=4 and day=9) or (year=2020 and month=4 and day=10) or (year=2020 and month=4 and day=11) or (year=2020 and month=4 and day=12) or (year=2020 and month=4 and day=13) or (year=2020 and month=4 and day=14) or  1=2  )
```
3. Cretae count analysis table.
```
spark.sql(s"""
create table if not exists secure_analysis.datalake_rawsink_count_analysis_updated(
tenant varchar(40),
domain_name varchar(40),
table_name varchar(40),
table_type varchar(40),
metric_name varchar(40),
metric_count int,
run_datetime varchar(40))
""")
```
