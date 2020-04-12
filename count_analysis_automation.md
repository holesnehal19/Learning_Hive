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

> import org.apache.spark.sql.catalyst.expressions.CurrentDate
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


