package model

import connectors.Parser
import org.apache.avro.generic.GenericRecord

case class CityAttributeItem(city: String, country: String, timeOffset: String) extends Serializable

/**
  * City Attribute item
  */
object CityAttributeItem {
  def From(tuple: (String, String, String)): CityAttributeItem = CityAttributeItem(tuple._1, tuple._2, tuple._3)

  def From(array: Array[String]): CityAttributeItem = CityAttributeItem(array(0), array(1), array(2))

  def From(record: GenericRecord): CityAttributeItem = CityAttributeItem(record.get("City").toString, record.get("country").toString, record.get("timezone").toString)
}

class CityAttributeItemParser extends Parser[CityAttributeItem] {
  override def parse(input: Array[String]): CityAttributeItem = CityAttributeItem.From(input)

  override def parse(input: GenericRecord): CityAttributeItem = CityAttributeItem.From(input)
}