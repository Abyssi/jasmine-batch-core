package model

import java.util.Calendar

import org.apache.avro.generic.GenericRecord
import utils.DateUtils

@SerialVersionUID(100L)
class CityCountryValueSample(val datetime: Calendar, val city: String, val country: String, val value: Double) extends Serializable {
}

object CityCountryValueSampleParser {
  def FromStringTuple(tuple: (String, String, String, String)): CityCountryValueSample = {
    new CityCountryValueSample(DateUtils.parseCalendar(tuple._1), tuple._2, tuple._3, tuple._4.toDouble)
  }

  def FromStringArray(array: Array[String]): CityCountryValueSample = {
    new CityCountryValueSample(DateUtils.parseCalendar(array(0)), array(1), array(2), array(3).toDouble)
  }

  def FromGenericRecord(record: GenericRecord): CityCountryValueSample = {
    new CityCountryValueSample(DateUtils.parseCalendar(record.get("datetime").toString), record.get("city").toString, record.get("country").toString, record.get("value").toString.toDouble)
  }
}
