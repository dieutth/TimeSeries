import implementation.{OutputRecord, TimeSeries}
import org.scalatest.FunSuite

import scala.io.Source

/**
  * @author dieutth, 12.07.2018
  *
  * TimeSeriesTest checks the correctness of TimeSeries in different cases.
  */
class TimeSeriesTest extends FunSuite{

  private def generateSourceFromText(text: String): Iterator[(Long, Double)] = {
    val arr =
      text.split("\r\n")
      .map{
        line =>
          val splitted = line.split(" ")
          (splitted(0).toLong, splitted(1).toDouble)
      }
      arr.iterator
  }

  test("Window-size = 60, less than window-size items in all rolling window"){
    val expectedResult = Array(
      OutputRecord(1355270609, 1.80215, 1, 1.80215, 1.80215, 1.80215),
      OutputRecord(1355270621, 1.80185, 2, 3.604, 1.80185, 1.80215),
      OutputRecord(1355270646, 1.80195, 3, 5.40595, 1.80185, 1.80215),
      OutputRecord(1355270702, 1.80225, 2, 3.6042, 1.80195, 1.80225),
      OutputRecord(1355270702, 1.80215, 3, 5.40635, 1.80195, 1.80225),
      OutputRecord(1355270829, 1.80235, 1, 1.80235, 1.80235, 1.80235),
      OutputRecord(1355270854, 1.80205, 2, 3.6044, 1.80205, 1.80235),
      OutputRecord(1355270868, 1.80225, 3, 5.40665, 1.80205, 1.80235),
      OutputRecord(1355271000, 1.80245, 1, 1.80245, 1.80245, 1.80245),
      OutputRecord(1355271023, 1.80285, 2, 3.6053, 1.80245, 1.80285),
      OutputRecord(1355271024, 1.80275, 3, 5.40805, 1.80245, 1.80285),
      OutputRecord(1355271026, 1.80285, 4, 7.2109, 1.80245, 1.80285),
      OutputRecord(1355271027, 1.80265, 5, 9.01355, 1.80245, 1.80285),
      OutputRecord(1355271056, 1.80275, 6, 10.8163, 1.80245, 1.80285),
      OutputRecord(1355271428, 1.80265, 1, 1.80265, 1.80265, 1.80265),
      OutputRecord(1355271466, 1.80275, 2, 3.6054, 1.80265, 1.80275),
      OutputRecord(1355271471, 1.80295, 3, 5.40835, 1.80265, 1.80295),
      OutputRecord(1355271507, 1.80265, 3, 5.40835, 1.80265, 1.80295),
      OutputRecord(1355271562, 1.80275, 2, 3.6054, 1.80265, 1.80275),
      OutputRecord(1355271588, 1.80295, 2, 3.6057, 1.80275, 1.80295)
    )
    val filepath = "\\data_scala.txt"
    val fileLines = Source.fromInputStream(getClass.getResourceAsStream(filepath)).getLines()
    val inputSource: Iterator[(Long, Double)] = fileLines.flatMap{
      line =>
        try {
          val splitted = line.split("\t")
          Some(splitted(0).toLong, splitted(1).toDouble)
        }catch {
          case e: Exception=> None
        }
    }

    val ts = TimeSeries(60)
    val actualResult =
      for (line <- inputSource.toArray.slice(0, 21))
        yield{
          val input = (line._1, line._2)
          ts.processLine(input)
        }
    val zipped = actualResult zip expectedResult
    for (p <- zipped)
      assert(p._1 == p._2)
  }

  test("Window-size = 3, array is always full"){
    val ts = TimeSeries(3)
    val inputSource = generateSourceFromText(
      """|0 0.0
        |1 1.0
        |2 2.0
        |3 3.0
        |4 4.0
        |5 5.0
        |6 6.0
        |7 7.0""".stripMargin)

    val expectedResult = Array(
      OutputRecord(0, 0.0, 1, 0.0, 0.0, 0.0),
      OutputRecord(1, 1.0, 2, 1.0, 0.0, 1.0),
      OutputRecord(2, 2.0, 3, 3.0, 0.0, 2.0),
      OutputRecord(3, 3.0, 3, 6.0, 1.0, 3.0),
      OutputRecord(4, 4.0, 3, 9.0, 2.0, 4.0),
      OutputRecord(5, 5.0, 3, 12.0, 3.0, 5.0),
      OutputRecord(6, 6.0, 3, 15.0, 4.0, 6.0),
      OutputRecord(7, 7.0, 3, 18.0, 5.0, 7.0)

    )
    val actualResult =
      for (line <- inputSource.toArray)
        yield{
          val input = (line._1, line._2)
          ts.processLine(input)
        }
    val zipped = actualResult zip expectedResult
    for (p <- zipped)
      assert(p._1 == p._2)
  }


  test("Window-size = 3, array always has only 1 valid event"){
    val ts = TimeSeries(3)
    val inputSource = generateSourceFromText(
      """|0 0.0
        |3 3.0
        |6 6.0
        |9 9.0
        |12 12.0
        |15 15.0
        |18 18.0""".stripMargin)

    val expectedResult = Array(
      OutputRecord(0, 0.0, 1, 0.0, 0.0, 0.0),
      OutputRecord(3, 3.0, 1, 3.0, 3.0, 3.0),
      OutputRecord(6, 6.0, 1, 6.0, 6.0, 6.0),
      OutputRecord(9, 9.0, 1, 9.0, 9.0, 9.0),
      OutputRecord(12, 12.0, 1, 12.0, 12.0, 12.0),
      OutputRecord(15, 15.0, 1, 15.0, 15.0, 15.0),
      OutputRecord(18, 18.0, 1, 18.0, 18.0, 18.0)
    )
    val actualResult =
      for (line <- inputSource.toArray)
        yield{
          val input = (line._1, line._2)
          ts.processLine(input)
        }
    val zipped = actualResult zip expectedResult
    for (p <- zipped)
      assert(p._1 == p._2)
  }

  test("Window-size = 3, array always has 2 valid event"){
    val ts = TimeSeries(3)
    val inputSource = generateSourceFromText(
      """|0 0.0
         |2 2.0
         |4 4.0
         |6 6.0
         |8 8.0
         |10 10.0
         |12 12.0""".stripMargin)

    val expectedResult = Array(
      OutputRecord(0, 0.0, 1, 0.0, 0.0, 0.0),
      OutputRecord(2, 2.0, 2, 2.0, 0.0, 2.0),
      OutputRecord(4, 4.0, 2, 6.0, 2.0, 4.0),
      OutputRecord(6, 6.0, 2, 10.0, 4.0, 6.0),
      OutputRecord(8, 8.0, 2, 14.0, 6.0, 8.0),
      OutputRecord(10, 10.0, 2, 18.0, 8.0, 10.0),
      OutputRecord(12, 12.0, 2, 22.0, 10.0, 12.0)
    )
    val actualResult =
      for (line <- inputSource.toArray)
        yield{
          val input = (line._1, line._2)
          ts.processLine(input)
        }
    val zipped = actualResult zip expectedResult
    for (p <- zipped)
      assert(p._1 == p._2)
  }


  test("Window-size = 3, array has a mixed number valid event"){
    val ts = TimeSeries(3)
    val inputSource = generateSourceFromText(
      """|0 0.0
         |1 1.0
         |2 2.0
         |3 3.0
         |5 5.0
         |6 6.0
         |8 8.0
         |12 12.0
         |15 15.0""".stripMargin)

    val expectedResult = Array(
      OutputRecord(0, 0.0, 1, 0.0, 0.0, 0.0),
      OutputRecord(1, 1.0, 2, 1.0, 0.0, 1.0),
      OutputRecord(2, 2.0, 3, 3.0, 0.0, 2.0),
      OutputRecord(3, 3.0, 3, 6.0, 1.0, 3.0),
      OutputRecord(5, 5.0, 2, 8.0, 3.0, 5.0),
      OutputRecord(6, 6.0, 2, 11.0, 5.0, 6.0),
      OutputRecord(8, 8.0, 2, 14.0, 6.0, 8.0),
      OutputRecord(12, 12.0, 1, 12.0, 12.0, 12.0),
      OutputRecord(15, 15.0, 1, 15.0, 15.0, 15.0)
    )
    val actualResult =
      for (line <- inputSource.toArray)
        yield{
          val input = (line._1, line._2)
          ts.processLine(input)
        }
    val zipped = actualResult zip expectedResult
    for (p <- zipped)
      assert(p._1 == p._2)
  }

}
