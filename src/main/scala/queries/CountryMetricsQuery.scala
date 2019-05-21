package queries

import java.util.Calendar

import model.{CityCountryValueSample, YearMonthCountryMetricsItem, YearMonthCountryMetricsItemParser}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

/**
  *
  **/
object CountryMetricsQuery {
  def run(input: RDD[CityCountryValueSample]): RDD[YearMonthCountryMetricsItem] = {
    input
      .map(item => ((item.datetime.get(Calendar.YEAR), item.datetime.get(Calendar.MONTH) + 1, item.country), StatCounter.apply(item.value))) // map to ((year, month, country), stat_counter)
      .reduceByKey(_.merge(_)) // aggregate stat counters
      .map(item => YearMonthCountryMetricsItemParser.FromTuple((item._1, (item._2.mean, item._2.stdev, item._2.min, item._2.max)))) // map to ((year,month,country),(mean,stdev,min,max))
  }

}
