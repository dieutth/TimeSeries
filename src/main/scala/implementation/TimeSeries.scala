package implementation

import scala.io.Source

/**
  * @author dieutth, 12.07.2018
  *
  *  TimeSeries: produces statistics of all events (coming from a "stream" of events)
  *  that belong to the same rolling window, where:
  *  - An event is a Tuple2 contains a timestamp (Long) and a measure (Double)
  *  - A rolling window size windowSize of an event X is the set of all events that
  *  have timestamp in the range from (X.timestamp - windowSize) (exclusive)
  *  to X.timestamp (inclusive).
  *
  *  TimeSeries works by keeping an internal array to store windowSize number of events.
  *  This internal array behaves as a cyclic buffer of fixed size (= windowSize).
  *  It uses an index (end) and a boolean flag (isFull) to keep track of the start/end
  *  of the array.
  *  - When array is not full yet (ie. number of events is less than windowSize), index
  *  of the first event is always 0, and index of last event is end-1
  *  - When array is full: index of the first event is end, and index of last event is
  *  end-1 cyclicly.
  *
  *  TimeSeries produces different stats for its events. In this particular implementation,
  *  it generates min, max, count, sum of all events in a rolling window.
  *
  * @param windowSize the maximum number of events to be kept in the buffer of TimeSeries
  */
class TimeSeries(windowSize: Int){

  private val buffer = Array.ofDim[(Long, Double)](windowSize)
  private var end = 0
  private var isFull = false

  /**
    * Append an event to the buffer.
    * It is always be added to the end side of the buffer, circularly.
    *
    * @param event: event to be appended to the buffer
    */
  private def append(event: (Long, Double)): Unit = {

    if (!isFull && end < windowSize){
      buffer(end) = event
      end += 1

    }else{

      //this if-branch is executed only once when the windowSize+1-th event arrive
        if (!isFull){
          isFull = true
          end = 0
        }
        buffer(end) = event
        end += 1
        if (end == windowSize)
          end = 0
    }
  }

  /**
    * Produce stats of all current valid events in the buffer.
    * An invalid event might stay in the buffer if the number of
    * @return
    */
  private def produceStats: OutputRecord = {
    var ind = if (end == 0) windowSize-1 else end-1
    val T = buffer(ind)._1
    val V = buffer(ind)._2
    var minV, maxV, RS = V
    var N = 1
    ind -= 1
    var flag = true

    // Rules to produce stats
    def update: Unit = {
      if (T - buffer(ind)._1 < windowSize) {
        val otherV = buffer(ind)._2
        minV = minV.min(otherV)
        maxV = maxV.max(otherV)
        RS += otherV
        N += 1
        ind -= 1
      } else {
        flag = false
      }
    }

    // find the earliest possible event that still belong to the
    // windowSize time range
    if (!isFull) {
      while (ind >= 0 && flag) {
        update
      }
    } else {
      while (ind >= 0 && flag) {
        update
      }

      //if end==0, all events are already checked from the previous while-loop
      if (end > 0) {
        ind = windowSize - 1
        while (ind >= end && flag) {
          update
        }
      }
    }
    OutputRecord(T, V.roundToPrec(5), N, RS.roundToPrec(5), minV.roundToPrec(5), maxV.roundToPrec(5))
  }

  /**
    * Process the latest event coming from data source
    * @param input the latest event to arrive to be processed
    * @return
    */
  def processLine(input: (Long, Double)): OutputRecord = {
    append((input._1, input._2))
    produceStats
  }


  /**
    * Produce a stream of event from text file
    * Ignore line that doesn't have the correct format
    * @param filepath absolute path to input file
    * @return
    */
  def processFileDatasource(filepath: String): Iterator[(Long, Double)] ={

    val fileLines =  Source.fromFile(filepath).getLines()
    val converted: Iterator[(Long, Double)] = fileLines.flatMap{
      line =>
        try {
          val splitted = line.split("\t")
          Some(splitted(0).toLong, splitted(1).toDouble)
        }catch {
          case e: Exception=> None
        }
    }
    converted
  }


  implicit class Helper(d: Double){
    /**
      * Round up a double to precision p (p digits after comma)
       * @param p precision
      * @return value after rounded up
      */
    def roundToPrec(p: Int): Double = {
      BigDecimal(d).setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
  }

}

object TimeSeries{
  def apply(windowSize: Int): TimeSeries = new TimeSeries(windowSize)
}
