package model

import java.util.Calendar

import connectors.Parser
import org.apache.avro.generic.GenericRecord
import utils.DateUtils

case class CityCountryValueSample(datetime: Calendar, city: String, country: String, value: Double) extends Serializable

object CityCountryValueSample {
  def From(tuple: (String, String, String, String, String)): CityCountryValueSample = CityCountryValueSample(DateUtils.parseCalendar(tuple._1, tuple._2), tuple._3, tuple._4, tuple._5.toDouble)

  def From(array: Array[String]): CityCountryValueSample = CityCountryValueSample(DateUtils.parseCalendar(array(0), array(1)), array(2), array(3), array(4).toDouble)

  def From(record: GenericRecord): CityCountryValueSample = CityCountryValueSample(DateUtils.parseCalendar(record.get("datetime").toString, record.get("timezone").toString), record.get("city").toString, record.get("country").toString, record.get("value").toString.toDouble)
}

class CityCountryValueSampleParser extends Parser[CityCountryValueSample] {
  override def parse(input: Array[String]): CityCountryValueSample = CityCountryValueSample.From(input)

  override def parse(input: GenericRecord): CityCountryValueSample = CityCountryValueSample.From(input)
}