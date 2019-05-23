package model

import java.util.Calendar

import org.apache.avro.generic.GenericRecord
import utils.DateUtils

class CityDescriptionSample(val datetime: Calendar, val city: String, val description: String) extends Serializable {
}

object CityDescriptionSampleParser {
  def FromStringTuple(tuple: (String, String, String)): CityDescriptionSample = {
    new CityDescriptionSample(DateUtils.parseCalendar(tuple._1), tuple._2, tuple._3)
  }

  def FromStringArray(array: Array[String]): CityDescriptionSample = {
    new CityDescriptionSample(DateUtils.parseCalendar(array(0)), array(1), array(2))
  }

  def FromGenericRecord(record: GenericRecord): CityDescriptionSample = {
    new CityDescriptionSample(DateUtils.parseCalendar(record.get("datetime").toString), record.get("city").toString, record.get("description").toString)
  }
}
