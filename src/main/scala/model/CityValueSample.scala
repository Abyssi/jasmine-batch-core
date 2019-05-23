package model

import java.util.Calendar

import org.apache.avro.generic.GenericRecord
import utils.DateUtils

class CityValueSample(val datetime: Calendar, val city: String, val value: Double) extends Serializable {
}

object CityValueSampleParser {
  def FromStringTuple(tuple: (String, String, String)): CityValueSample = {
    new CityValueSample(DateUtils.parseCalendar(tuple._1), tuple._2, tuple._3.toDouble)
  }

  def FromStringArray(array: Array[String]): CityValueSample = {
    new CityValueSample(DateUtils.parseCalendar(array(0)), array(1), array(2).toDouble)
  }

  def FromGenericRecord(record: GenericRecord): CityValueSample = {
    new CityValueSample(DateUtils.parseCalendar(record.get("datetime").toString), record.get("city").toString, record.get("value").toString.toDouble)
  }
}
