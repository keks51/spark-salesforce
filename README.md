# Spark Salesforce Connector
A library for connecting Spark with Salesforce
## Requirements
This library requires Spark 2.x.
## Linking
This package can be added to Spark using the `--packages` command line option. For example, to include it when starting the spark shell:

## Using with Spark shell

## Features
Salesforce Data Source for Spark supports reading(writing is not yet implemented) of Salesforce data from Spark SQL.
* **Automatic schema conversion** -
  it supports most conversions between Spark SQL and salesforce records except (address, location) since they are returned with null values by the SOAP API and not supported at all by the Bulk API
* **Stream processing** - only structured streaming
* **Batch processing** - reading salesforce data in batches
* **Soap connection** - loading data via soap
* **Supporting Spark Datasource Api v1 and v2** - supporting old and new datasource api
* **Custom spark UI for query metrics** - additional streaming metrics in spark ui
## Limitations
- Aggregations in SOQL query are not supported. Use Spark aggregations
## Supported types for Salesforce -> Spark SQL conversion
|           SF type          | Spark SQL type |
|----------------------------|----------------|
| string                     | StringType     |
| picklist                   | StringType     |
| multipicklist              | StringType     |
| reference                  | StringType     |
| combobox                   | StringType     |
| base64                     | StringType     |
| textarea                   | StringType     |
| currency                   | StringType     |
| percent                    | StringType     |
| phone                      | StringType     |
| id                         | StringType     |
| time                       | StringType     |
| url                        | StringType     |
| email                      | StringType     |
| encryptedstring            | StringType     |
| datacategorygroupreference | StringType     |
| anyType                    | StringType     |
| complexvalue               | StringType     |
| date                       | StringType     |
| datetime                   | TimestampType  |
| int                        | IntegerType    |
| double                     | DoubleType     |
| boolean                    | BooleanType    |

## Options
### Connection settings
* `userName`: Salesforce Username.
* `userPassword`: Salesforce user Password. Please append security token along with a password. For example,
  if a user’s password is mypassword, and the security token is XXXXXXXXXX, the user must provide mypasswordXXXXXXXXXX.
* `authEndPoint`: (Optional) Salesforce Login URL. Default value login.salesforce.com
* `apiVersion`:   used in for example "https://login.salesforce.com/services/Soap/u/<apiVersion>". apiVersion like 39.0.
* `proxyHost`:  (Optional) proxy configuration.
* `proxyPort`:  (Optional) proxy configuration.
* `proxyUserName`: (Optional) proxy configuration.
* `proxyPassword`: (Optional) proxy configuration.
* `useHttps`: (Optional) use https or http in salesforce url. Default (true).
* `connectionTimeout`: (Optional) salesforce login connection timeout in milliseconds (Long). Default (30000).
* `checkConnectionRetries`: (Optional) number of get connection retries before aborting the loading (INTEGER). Default (10).
* `retrySleepMin`: (Optional) min number of milliseconds to sleep before trying to load again if an exception occurs. Default (5000).
* `retrySleepMax`: (Optional) max number of milliseconds to sleep before trying to load again if an exception occurs. Default (20000).
### Query settings
* `isQueryAll`:  (Optional) Include deleted records or not. Default value (true).
* `offsetColumn`: (Optional) is used to load partitioned data and to check updates while streaming. 
  `String type` like `id column` is not supported and data will be loaded only in one partition. Default (SystemModstamp)
* `initialOffset`: (Optional) offsetColumn value of the first record. Default (automatically querying salesforce table to get the initial offset)
* `endOffset`: (Optional) offsetColumn value of the last record. Default (automatically querying salesforce table to get the last offset)
* `loadNumPartitions`: (Optional) number of spark partition to load data in parallel (INTEGER). Default (1)  
  Due to 'Invalid query locator' issue, the recommendation is to set at most 10. 
* `queryExecutorClassName`: (Optional) query execution logic. Custom logic can be implemented by extending com.keks.sf.soap.SoapQueryExecutor
### Streaming settings
* `streamingMaxRecordsInPartition`: (Optional) maximum number of records to load in one spark partition (INTEGER). Is used in incremental ingesting. Default (10000).
* `streamingMaxBatches`: (Optional) maximum number of soap batches to load in one spark partition (INTEGER). Is used to prevent loading skewed data in one partition. Default (10000).
* `streamingAdditionalWaitWhenIncrementalLoading`: (Optional) time to wait in milliseconds while requesting new data during incremental ingesting. Default (600000).
* `streamingLoadAvailableData`: (Optional) loading all available data and stopping streaming or continue loading  data in incremental mode. Default (false).
* `streamingQueryName`: (Optional) set query name to see query metrics in spark UI.

## Architecture overview
query example: `"select id, name from account where name = 'alex'"`
### Spark batch
On Driver side:  
1) getting the `offsetColumn` from configuration or taking default (Systemmodstamp).
2) getting the `initial offset` from conf or salesforce table.
3) getting the `last offset` from conf or salesforce table.
4) Splitting query in partitions by 'loadNumPartitions' for example `3` and adding an `'order by' clause`   
   `"select id, name from account where (name = 'alex') AND (SystemModstamp >= 2011-01-01T00:00:00.000Z AND SystemModstamp < 2015-01-01T00:00:00.000Z) ORDER BY SystemModstamp"`  
   `"select id, name from account where (name = 'alex') AND (SystemModstamp >= 2015-01-01T00:00:00.000Z AND SystemModstamp < 2017-01-01T00:00:00.000Z) ORDER BY SystemModstamp"`  
   `"select id, name from account where (name = 'alex') AND (SystemModstamp >= 2017-01-01T00:00:00.000Z AND SystemModstamp < 2020-01-01T00:00:00.000Z) ORDER BY SystemModstamp"`  
5) executing each query on Executor in parallel.
Each executor keeps the last offset value from the previous soap batch to continue loading from this offset if an exception occurs.  
This approach can produce duplicates but guarantee the data consistency.  
#### Limitations
Spark job can fail if table is large(2 millions rows with huge number of columns, more then 100) and skewed(90% of records are in one partition).
To load large table use load once streaming approach.
### Spark streaming
Streaming is a micro batch processing. So the first steps are the same as in spark batch.  
Each executor loads data until `streamingMaxRecordsInPartition` threshold. Then the micro batch job finishes, and the executor's metrics like:  
- last batch offset value
- next batch query locator
- number of loaded records
- was all data loaded or not  
are saved in the checkPoint dir.  
Driver loads these metrics from the checkpoint dir.  
1) if all partitions still have data to load then only lower bounds offsets are changed  
  for example `(SystemModstamp >= 2011-01-01T00:00:00.000Z AND...` is changed to `(SystemModstamp >= <last offset value> AND...`
2) if some partitions loaded all available data then all partitions are recreated:  
  before:  
    `age >= 0 and age < 10`  finished  
    `age >= 10 and age < 20` finished  
    `age >= 20 and age < 30` not finished. last value is 12  
   recreated to:  
   `age >= 12 and age < 14`  
   `age >= 14 and age < 17`  
   `age >= 17 and age < 20`  
This algorithm is repeated until all data between `initial offset` and `end offset` is loaded. 
Recreating partitions approach allow to load large and skewed tables with minimum of cluster resources.  
if option `streamingLoadAvailableData` is true then spark streaming job is finished.
Else new data is polling each `streamingAdditionalWaitWhenIncrementalLoading` and the number of partitions is calculated by: 
`math.min(sfOptions.sfLoadNumPartitions, math.ceil(newRecords / sfOptions.streamingMaxRecordsInPartition))`  
#### Limitations
Watermarks are not supported. They can be supported when loading data in one partition but this approach is not yet implemented
DStreams are not supported.
ContinuousReadSupport is not supported.

## Examples
All options can be set via `--conf`. Just add `spark.sf` at the begging. For example, `--conf 'spark.sf.userName'`
To query all columns asterisks can be used. For example, "select * from user"  
### Batch:
```scala
spark
  .read
  .option(SF_USER_NAME, "keks")
  .option(SF_USER_PASSWORD, "password")
  .format(SALESFORCE_SOAP_V2)
  .load("SELECT * FROM User WHERE Name = 'keks'")
  .filter(col("age") === 25) // this filter is pushed down and salesforce query is "SELECT * FROM User WHERE (Name = 'keks') AND (Age = 25)" 
```
```scala
spark
  .read
  .option(SF_USER_NAME, "keks")
  .option(SF_USER_PASSWORD, "password")
  .format(SALESFORCE_SOAP_V2)
  .load("SELECT * FROM User WHERE Name = 'keks'")
  .select("Id", "Name") // this select is pruned and salesforce query is "SELECT Id,Name FROM User WHERE (Name = 'keks')" 
```
```scala
 val schema = StructType(Array(
  StructField(SYSTEMMODSTAMP, TimestampType),
  StructField(ID, StringType),
  StructField(NAME, StringType)
  ))
spark
  .read
  .option(SF_USER_NAME, "keks")
  .option(SF_USER_PASSWORD, "password")
  .format(SALESFORCE_SOAP_V2)
  .schema(schema)
  .load("SELECT * FROM User") // select is pruned due to .schema(...) and salesforce query is "SELECT SystemModstamp,Id,Name FROM User" 
```
### Streaming
load once
```scala
val streamDF = spark
  .readStream
  .option(SF_STREAMING_QUERY_NAME, "User")
  .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = true)
  .option(SF_STREAMING_MAX_BATCHES, 2)
  .format(SALESFORCE_SOAP_V2)
  .load("SELECT * FROM User WHERE Name = 'keks'")
val query = streamDF.writeStream
  .outputMode(OutputMode.Append)
  .format("parquet")
  .option("path", saveDir)
  .option("checkpointLocation", s"...")
  .start()
query.processAllAvailable() // this is need to be set along  with .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = true)
query.stop()
```
incremental loading
```scala
val streamDF = spark
  .readStream
  .option(SF_STREAMING_QUERY_NAME, "User")
  .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = false)
  .option(SF_STREAMING_ADDITIONAL_WAIT_WHEN_INCREMENTAL_LOADING, 100000)
  .option(SF_STREAMING_MAX_BATCHES, 2)
  .format(SALESFORCE_SOAP_V2)
  .load("SELECT * FROM User WHERE Name = 'keks'")
val query = streamDF.writeStream
  .outputMode(OutputMode.Append)
  .format("parquet")
  .option("path", saveDir)
  .option("checkpointLocation", s"...")
  .start()
query.awaitTermination()  // this is need to be set along  with .option(SF_STREAMING_LOAD_AVAILABLE_DATA, value = false)
query.stop()
```
