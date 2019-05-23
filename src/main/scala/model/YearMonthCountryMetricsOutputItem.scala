package model

import utils.JsonSerializable

case class Metrics(mean: Double, stdev: Double, min: Double, max: Double) extends Serializable with JsonSerializable

case class YearMonthCountryMetricsOutputItem(year: Int, month: Int, country: String, metrics: Metrics) extends Serializable with JsonSerializable

object YearMonthCountryMetricsOutputItem {
  def From(tuple: ((Int, Int, String), (Double, Double, Double, Double))): YearMonthCountryMetricsOutputItem = YearMonthCountryMetricsOutputItem(tuple._1._1, tuple._1._2, tuple._1._3, Metrics(tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4))
}
