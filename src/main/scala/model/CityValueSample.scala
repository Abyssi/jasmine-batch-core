package model

import java.util.Calendar

import connectors.Parser
import org.apache.avro.generic.GenericRecord
import utils.DateUtils

/**
  * City Value Sample
  * @param datetime
  * @param city
  * @param value
  */
case class CityValueSample(datetime: Calendar, city: String, value: Double) extends Serializable

object CityValueSample {
  def From(tuple: (String, String, String, String)): CityValueSample = CityValueSample(DateUtils.parseCalendar(tuple._1, tuple._2), tuple._3, tuple._4.toDouble)

  def From(array: Array[String]): CityValueSample = CityValueSample(DateUtils.parseCalendar(array(0), array(1)), array(2), array(3).toDouble)

  def From(record: GenericRecord): CityValueSample = CityValueSample(DateUtils.parseCalendar(record.get("datetime").toString, record.get("timezone").toString), record.get("city").toString, record.get("value").toString.toDouble)
}

class CityValueSampleParser extends Parser[CityValueSample] {
  override def parse(input: Array[String]): CityValueSample = CityValueSample.From(input)

  override def parse(input: GenericRecord): CityValueSample = CityValueSample.From(input)
}