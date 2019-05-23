package model

import utils.JsonSerializable

@SerialVersionUID(100L)
class CountryCityRankItem(val position: Int, val value: Double) extends Serializable with JsonSerializable {
}

@SerialVersionUID(100L)
class CountryCityRankCompareItem(val country: String, val city: String, val newRank: CountryCityRankItem, val oldRank: CountryCityRankItem) extends Serializable with JsonSerializable {
}

object CountryCityRankCompareItemParser {
  def FromTuple(tuple: ((String, String), ((Int, Double), (Int, Double)))): CountryCityRankCompareItem = {
    new CountryCityRankCompareItem(tuple._1._1, tuple._1._2, new CountryCityRankItem(tuple._2._1._1, tuple._2._1._2), new CountryCityRankItem(tuple._2._2._1, tuple._2._2._2))
  }
}
