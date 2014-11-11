# HBase TAggregator

HBase Aggregator implementation for timeseries based aggregations.

It is meant to allow for much faster aggregations when time intervals are at play. For example, when aggregating a day's worth of data down to 15min averages, you'd need 96 queries doing 96 scans using the standard aggregate implementation. TAggregator will do the same thing requiring just one query using a single scan, producing a Map of 96 TimeStamp-Value assignments.

## Table of Contents

 - [Supported Features](#supported-features)
 - [TODO](#todo)
 - [Planned Features](#planned-features)
 - [Usage](#usage)
 - [For Maven Users](#for-maven-users)
 - [Licensing](#licensing)

## Supported Features

* Max
* Min
* Sum
* Avg
* Will run with 0.98, no other versions tested, so far

## TODO

* Fix pom (Some minor issues remaining, like protocol buffers not getting built in time for packaging, but nothing serious anymore)
* Add more test cases for limits provided via scan
* Fix Javadocs
* Currently, when running with a scanner provided timerange, the result will be different from when running with a directly supplied range. The reason for this is, that when providing a range à la 10/10/2014 00:00-02:00 the scanner will cut off at 02:00, whereas the directly supplied range will also give the result for the range starting at 02:00.

## Planned Features

* Weighted avg
* Some sort of diff

## Usage

```java
int interval = 900;
int time_min = (int) ((new GregorianCalendar(2014, 10, 10, 0, 0, 0).getTime().getTime()) / 1000);
int time_max = (int) ((new GregorianCalendar(2014, 10, 10, 1, 59, 59).getTime().getTime()) / 1000);
String KEY_FILTER_PATTERN = "00000001111"; // a 4-byte-timestamp (int) is assumed; Mask it using 1s 


Scan scan = new Scan();
scan.addFamily(FAMILY);
TimeseriesAggregationClient tsac = new TimeseriesAggregationClient(conf, interval, time_min, time_max, KEY_FILTER_PATTERN);

final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
ConcurrentSkipListMap<Long, Long> maximum = tsac.max(TEST_TABLE, ci, scan);
```

*Note:* For the KEY_FILTER_PATTERN it is assumed, that somewhere in your Key, there is an *Integer* timestamp (4 bytes; in seconds). The pattern masks the position of this integer in the key using 1s. Naturally, it is also assumed, that Keys handled during this operation are fixed length.

Alternatively, you can provide the total time range (time_min,time_max) via the scan object. In this case, you do not provide it to the Coprocessor, itself:


```java
int interval = 900;
int time_min = (int) ((new GregorianCalendar(2014, 10, 10, 0, 0, 0).getTime().getTime()) / 1000);
int time_max = (int) ((new GregorianCalendar(2014, 10, 10, 1, 59, 59).getTime().getTime()) / 1000);


Scan scan = new Scan();
scan.addFamily(FAMILY);
scan.setTimeRange(time_min, time_max)
TimeseriesAggregationClient tsac = new TimeseriesAggregationClient(conf, interval);

final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
ConcurrentSkipListMap<Long, Long> maximum = tsac.max(TEST_TABLE, ci, scan);
```

## For Maven users

You can use this repo for maven, also. Kind of the poor many maven repo, it resides in the mvn-repo branch:

```xml
<repositories>
        <repository>
		<id>hbase-taggregator-mvn-repo</id>
		<url>https://raw.github.com/juwi/HBase-Taggregator/mvn-repo/</url>
		<snapshots>
			<enabled>true</enabled>
			<updatePolicy>always</updatePolicy>
		</snapshots>
	</repository>
</repositories>
```

## Licensing

Code and documentation Copyright Julian Wissmann, licensed under the Apache License version 2. 

The following sources have been pulled from HBase, directly and thus are licensed to the Apache Software Foundation under Apache Livense Version 2:

* src/main/protobuf/Cell.proto
* src/main/protobuf/Client.proto
* src/main/protobuf/Comparator.proto
* src/main/protobuf/Filter.proto
* src/main/protobuf/HBase.proto
