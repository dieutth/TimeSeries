package implementation

case class OutputRecord(T: Long, V: Double, N: Int, RS: Double, minV: Double, maxV: Double){
  override def toString: String = {
    T + "\t" + V + "\t" + N + "\t" + RS + "\t" + minV + "\t" + maxV
  }
}
