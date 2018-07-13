# TimeSeries

A simple program to print out to the standard output the statistics of events in a rolling window. 
## 1. How to Run
### Run with sbt
Navigate to the root folder (TimeSeriesTask) and execute:
> sbt "run /path/to/dataset/file"
### Test 
Execute:
> sbt test
to run test cases.
## 2. Some assumptions

 -  A rolling window of an event X(timestamp, measure) contains all events that has timestamp in the range (X.timestamp - windowSize, X.timestamp].
(ie. X.timestamp-windowSize is excluded and X.timestamp is included).
 - If a line in the file is invalid (empty or does not has correct format: a long value follow by a double value) then the line is simply ignored.
 
## 3. Area for improvement
The printing format is not entirely optimal now. It is done a bit manually and could possibly be improved by having some padding techniques to align the header with the data rows followed.

