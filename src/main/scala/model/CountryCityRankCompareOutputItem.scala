package model

import utils.JsonSerializable

case class CountryCityRankItem(position: Int, value: Double) extends Serializable with JsonSerializable

case class CountryCityRankCompareOutputItemItem(country: String, city: String, newRank: CountryCityRankItem, oldRank: CountryCityRankItem) extends Serializable with JsonSerializable

object CountryCityRankCompareOutputItemItem {
  def From(tuple: ((String, String), ((Int, Double), (Int, Double)))): CountryCityRankCompareOutputItemItem = CountryCityRankCompareOutputItemItem(tuple._1._1, tuple._1._2, CountryCityRankItem(tuple._2._1._1, tuple._2._1._2), CountryCityRankItem(tuple._2._2._1, tuple._2._2._2))
}
