package implementation

/**
  * @author dieutth, 13.07.2018
  *
  *  Application to print stats of TimeSeries to standard output console.
  */

object Application {
    def main(args: Array[String]): Unit = {
    if (args.length != 1){
      println(
        """
          |Please enter one valid input file path.
        """.stripMargin)
    }else{
      val filepath = args(0)

      val ts = TimeSeries(60)
      val inputSource = ts.processFileDatasource(filepath)

      val header = "T         \tV      \tN \tRS     \tMinV   \tMaxV     "
      val lineSeperator = Array.fill(header.length + 20)("-").mkString("")
      println(header)
      println(lineSeperator)
      while(inputSource.hasNext){
        val line = inputSource.next()
        val outputLine = ts.processLine(line)
        println(outputLine)
      }
    }
  }

}
