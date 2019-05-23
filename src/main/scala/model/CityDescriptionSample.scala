package model

import java.util.Calendar

import connectors.Parser
import org.apache.avro.generic.GenericRecord
import utils.DateUtils

case class CityDescriptionSample(datetime: Calendar, city: String, description: String) extends Serializable

object CityDescriptionSample {
  def From(tuple: (String, String, String)): CityDescriptionSample = CityDescriptionSample(DateUtils.parseCalendar(tuple._1), tuple._2, tuple._3)

  def From(array: Array[String]): CityDescriptionSample = CityDescriptionSample(DateUtils.parseCalendar(array(0)), array(1), array(2))

  def From(record: GenericRecord): CityDescriptionSample = CityDescriptionSample(DateUtils.parseCalendar(record.get("datetime").toString), record.get("city").toString, record.get("description").toString)
}

class CityDescriptionSampleParser extends Parser[CityDescriptionSample] {
  override def parse(input: Array[String]): CityDescriptionSample = CityDescriptionSample.From(input)

  override def parse(input: GenericRecord): CityDescriptionSample = CityDescriptionSample.From(input)
}
