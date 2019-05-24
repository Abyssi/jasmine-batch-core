package model

import connectors.Parser
import org.apache.avro.generic.GenericRecord

/**
  * City Value Item
  * @param datetime
  * @param city
  * @param value
  */
case class CityValueItem(datetime: String, city: String, value: String) extends Serializable

object CityValueItem {
  def From(tuple: (String, String, String)): CityValueItem = CityValueItem(tuple._1, tuple._2, tuple._3)

  def From(array: Array[String]): CityValueItem = CityValueItem(array(0), array(1), array(2))

  def From(record: GenericRecord): CityValueItem = CityValueItem(record.get("datetime").toString, record.get("city").toString, record.get("value").toString)
}

class CityValueItemParser extends Parser[CityValueItem] {
  override def parse(input: Array[String]): CityValueItem = CityValueItem.From(input)

  override def parse(input: GenericRecord): CityValueItem = CityValueItem.From(input)
}