package model

import utils.JsonSerializable

case class YearCityOutputItem(year: Int, city: String) extends Serializable with JsonSerializable

object YearCityOutputItem {
  def From(tuple: (Int, String)): YearCityOutputItem = YearCityOutputItem(tuple._1, tuple._2)
}
