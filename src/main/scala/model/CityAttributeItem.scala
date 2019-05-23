package model

import connectors.Parser
import org.apache.avro.generic.GenericRecord

case class CityAttributeItem ( city : String, country : String, timeOffset : String )

class CityAttributeItemParser extends Parser[CityAttributeItem]
{
  override def parse(input: Array[String]): CityAttributeItem = {
    CityAttributeItem(input(0), input(1), input(2))
  }

  override def parse(input: GenericRecord): CityAttributeItem = {
    CityAttributeItem(input.get(0).toString, input.get(1).toString, input.get(2).toString)
  }
}
