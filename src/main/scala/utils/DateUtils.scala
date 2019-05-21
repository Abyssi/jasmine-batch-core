package utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Date Utils
  */
object DateUtils {
  def parseCalendar(datetime: String, format: String = "yyyy-MM-dd'T'HH:mm:ssZ"): Calendar = {
    val sdf = new SimpleDateFormat(format)
    val calendar = Calendar.getInstance()
    calendar.setTime(sdf.parse(datetime))
    calendar
  }

  def reformatWithTimezone(datetime: String, offset: String): String = {
    datetime.replace(" ", "T") + offset
  }
}
