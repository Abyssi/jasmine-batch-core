package utils

object IterableUtils {
  def minMax(a: Iterable[Double]): (Double, Double) = {
    a.foldLeft((Double.MaxValue, Double.MinValue)) { case ((min: Double, max: Double), e: Double) => (math.min(min, e), math.max(max, e)) }
  }
}
