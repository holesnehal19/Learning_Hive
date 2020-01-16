# Learning_Hive

## difference between SQL and HQL

| SQL           | HQL           |
| ------------- | ------------- |
| based on a relational database model  | combination of object-oriented programming with relational database concepts  |
| data stored in tables and modifies its rows and columns  | it is concerned about objects and its properties  |
| relationship that exists between two tables | relation between two objects |

## difference between hive and spark sql


| hive           | spark sql    |
| ------------- | ------------- |
| first released in the year 2012 | first released in the year 2014 |
| version 2.3.2 | version 2.1.2 |

## Pig vs Hive vs Hadoop MapReduce

| Pig           | Hive           | Hadoop MR |
| ------------- | -------------  | --------- |
| it has the scripting language | it has SQL like Query language | has compiled language |
| it also has the High level of Abstraction | it has a Low level of Abstraction | has the High level of Abstraction |
| Comparatively less no. of the line of codes from MapReduce | Comparatively less no. of the line of codes from both MapReduce and Pig |  it has More line of codes |

## difference between hive and hbase

| hive           |  hbase       |
| ------------- | ------------- |
| Apache Hive is not a database | Base does support NoSQL database |
| Hive does support Batch processing | HBase does support real-time data streaming |
| schema is required | no need of schema |

- optimize Hive Performance :
```
Tez execution engine in hive
using suitable file format
use partitioning
use bucketing
hive indexing
```
- How can client interact with Hive? or How can we connect with hive?
1. Hive Thrift client
2. JDBC driver
3. ODBC drivr

- How Hive organize the data?
1. Tables
2. Partitions
3. Buckets

- How to add the partition in existing table without the partition table?
```
Basically, we cannot add/create the partition in the existing table, especially which was not partitioned while creation of the table.
Although, there is one possible way, using “PARTITIONED BY” clause. But the condition is if you had partitioned the existing table, then by using the ALTER TABLE command, you will be allowed to add the partition.
So, here are the create and alter commands:

CREATE TABLE tab02 (foo INT, bar STRING) PARTITIONED BY (mon STRING);
ALTER TABLE tab02 ADD PARTITION (mon =’10’) location ‘/home/hdadmin/hive-0.13.1-cdh5.3.2/examples/files/kv5.txt’;
```
- Explain Clustering in Hive?
```
to decompose table data sets into more manageable parts is Clustering in Hive.table is divided into the number of partitions, and these partitions can be further subdivided into more manageable parts known as Buckets/Clusters.  In addition, “clustered by” clause is used to divide the table into buckets.
```
- Unable to instantiate org.apache.hadoop.hive.metastore.HiveMetaStoreClient
```
There is a possibility that because of  following reasons above error may occur:

While we use derby metastore, Then lock file would be there in case of the abnormal exit.
Hence, do remove the lock file
rm metastore_db/*.lck

Moreover, Run hive in Debug mode
hive -hiveconf hive.root.logger=DEBUG,console
```
-  How many types of Tables in Hive?
1. managed table
2. external table

- Explain Hive Thrift server?
Thrift is a software framework. Also, it allows clients using languages including Java, C++, Ruby, and many others, to programmatically access Hive remotely.

- How to Write a UDF function in Hive?
```
 Basically, following are the steps:

> Create a Java class for the User Defined Function which extends ora.apache.hadoop.hive.sq.exec.UDF and implements more than one evaluate() methods. 
> Put in your desired logic and you are almost there.
> Package your Java class into a JAR file
> Go to Hive CLI, add your JAR, and verify your JARs is in the Hive CLI classpath
> CREATE TEMPORARY FUNCTION in Hive which points to your Java class
> Then Use it in Hive SQL.
```
- difference between sort by and order by
1. sort by :
            It used for large dataset. beacause it has multiple reducers. and it took less amount of time
2. order by :
            It used for small dataset. because it hase single reducer and it took more time to execute.
            
 - What is the importance of driver in hive
 It fetches the query from UI and JDBC interfaces to process the query.
 
- How to specify table creator name when creating the table 
  > using TBLPROPERTIES. TBLPROPERTIES('creator' = 'snehal')
  
- Hive Built-In Functions :
1. collection
2. conditional
3. date
4. mathematical
5. string

            
 
