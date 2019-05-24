package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

/**
  * Date Utils
  */
object DateUtils {
  //"yyyy-MM-dd'T'HH:mm:ssZ"
  def parseCalendar(datetime: String, timeZoneId: String, format: String = "yyyy-MM-dd HH:mm:ss"): Calendar = {
    val sdf = new SimpleDateFormat(format)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId))
    calendar.setTime(sdf.parse(datetime))
    calendar
  }

  def reformatWithTimezone(datetime: String, offset: String): String = {
    datetime.replace(" ", "T") + offset
  }
}
