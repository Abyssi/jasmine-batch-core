package model

import connectors.Parser
import org.apache.avro.generic.GenericRecord

case class CityValueItem(datetime: String, city: String, value: String)

class CityValueItemParser extends Parser[CityValueItem] {
  override def parse(input: Array[String]): CityValueItem = {
    CityValueItem(input(0), input(1), input(2))
  }

  override def parse(input: GenericRecord): CityValueItem = {
    CityValueItem(input.get(0).toString, input.get(1).toString, input.get(2).toString)
  }

}
