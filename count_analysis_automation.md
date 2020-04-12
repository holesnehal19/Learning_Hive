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
4. Insert into count analysis table.
```
spark.sql(s"""insert into secure_analysis.datalake_rawsink_count_analysis_updated
select * from (
select 
'venmo' as tenant,
'identity' as domain_name,
'auth_user' as table_name,
'class2' as table_type,
'kc2sd_count',
(select count(distinct (id,batch_id,dt_last_updated)) from dl_venmo_ss_identity_raw_secure_tables.auth_user_rawsink
where to_utc_timestamp(from_unixtime(dt_last_updated DIV 1000),'PST') >= '$date1'
and to_utc_timestamp(from_unixtime(dt_last_updated DIV 1000),'PST') < '$date2' and $finalRangeQuery),
(select DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))

UNION ALL

select 
'venmo' as tenant,
'identity' as domain_name,
'auth_user' as table_name,
'class2' as table_type,
'kc3p_count',
(select count (distinct (id,message_batch_id,dt_last_updated)) from dl_venmo_ss_identity_streaming_raw_secure_tables.auth_user_rawsink
where to_utc_timestamp(from_unixtime(dt_last_updated DIV 1000),'PST') >= '$date1'
and to_utc_timestamp(from_unixtime(dt_last_updated DIV 1000),'PST') < '$date2' and $finalRangeQuery),
(select DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
)
""").show(500, false)
```
Note - first table is filebased and second table is streaming based.

5. insert one more table entry.

```
spark.sql(s"""insert into secure_analysis.datalake_rawsink_count_analysis_updated
select * from (
select 
'venmo' as tenant,
'identity' as domain_name,
'id_number_coll' as table_name,
'class2' as table_type,
'kc2sd_count',
(select count(distinct(_id,batch_id,jsonstring_dt_updated)) from dl_venmo_ss_identity_raw_secure_tables.id_number_coll_rawsink
where to_utc_timestamp(from_unixtime(jsonstring_dt_updated DIV 1000),'PST') >= '$date1'
and to_utc_timestamp(from_unixtime(jsonstring_dt_updated DIV 1000),'PST') < '$date2' and $finalRangeQuery),
(select DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))

UNION ALL

select 
'venmo' as tenant,
'identity' as domain_name,
'id_number_coll' as table_name,
'class2' as table_type,
'kc3p_count',
(select count(distinct(_id,message_batch_id,jsonstring_dt_updated)) from dl_venmo_ss_identity_streaming_raw_secure_tables.id_number_coll_rawsink
where to_utc_timestamp(from_unixtime(jsonstring_dt_updated DIV 1000),'PST') >= '$date1'
and to_utc_timestamp(from_unixtime(jsonstring_dt_updated DIV 1000),'PST') < '$date2' and $finalRangeQuery),
(select DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
)
""")
```
6. drop report table.
```
spark.sql("""drop table if exists secure_analysis.datalake_rawsink_count_analysis_report_updated""")
```
7.  Create report table.
```
spark.sql(s"""
create table if not exists secure_analysis.datalake_rawsink_count_analysis_report_updated(
tenant varchar(40),
domain_name varchar(40),
table_name varchar(40),
CDM_flag varchar(40),
table_type varchar(40),
start_date varchar(40),
end_date varchar(40),
count_from varchar(40),
kc3p_count bigint,
kc2sd_count bigint,
difference varchar(40),
run_date varchar(40))
""").show(100,false)
```
8. Insert into report table.
```
spark.sql(s"""insert into secure_analysis.datalake_rawsink_count_analysis_report_updated 
select distinct * from ( 
(select 
    distinct tenant,domain_name,table_name,'Yes',table_type,'$date1','$date2','Rawsink',
    (select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc3p_count' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date and table_name='auth_user') as kc3p_count,
    (select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc2sd_count' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date and table_name='auth_user') as kc2sd_count,
    ((select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc3p_count' and table_name='auth_user' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date) - 
    (select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc2sd_count' and table_name='auth_user' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date)) as difference,
    (select DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
from 
    secure_analysis.datalake_rawsink_count_analysis_updated where table_name='auth_user' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date)
    
UNION ALL

(select 
    distinct tenant,domain_name,table_name,'Yes',table_type,'$date1','$date2','Rawsink',
    (select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc3p_count' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date and table_name='id_number_coll') as kc3p_count,
    (select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc2sd_count' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date and table_name='id_number_coll') as kc2sd_count,
    ((select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc3p_count' and table_name='id_number_coll' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date) - 
    (select metric_count from secure_analysis.datalake_rawsink_count_analysis_updated where metric_name='kc2sd_count' and table_name='id_number_coll' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date)) as difference,
    (select DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss'))
from 
    secure_analysis.datalake_rawsink_count_analysis_updated where table_name='id_number_coll' and DATE_FORMAT(run_datetime,'yyyy-MM-dd') = current_date)
)
""").show(100,false)
```
9. select count from report table.
