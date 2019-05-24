package queries

import java.util.Calendar

import model.{CityCountryValueSample, YearMonthCountryMetricsOutputItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

/**
  * Query 2
  *
  **/
object CountryMetricsQuery {
  def run(input: RDD[CityCountryValueSample]): RDD[YearMonthCountryMetricsOutputItem] = {
    input
      .map(item => ((item.datetime.get(Calendar.YEAR), item.datetime.get(Calendar.MONTH) + 1, item.country), item.value)) // map to ((year, month, country), value)
      .aggregateByKey(StatCounter(Nil))((acc: StatCounter, value: Double) => acc.merge(value), (acc1: StatCounter, acc2: StatCounter) => acc1.merge(acc2)) //aggregate with StatCounter
      .map(item => YearMonthCountryMetricsOutputItem.From((item._1, (item._2.mean, item._2.stdev, item._2.min, item._2.max)))) // map to ((year,month,country),(mean,stdev,min,max))
  }

}
