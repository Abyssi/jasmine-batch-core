package queries

import java.util.Calendar

import model.{CityCountryValueSample, CountryCityRankCompareItem, CountryCityRankCompareItemParser}
import operators.SorterFunctions
import operators.model.MeanCounter
import org.apache.spark.rdd.RDD

/**
  *
  **/
object MaxDiffCountriesQuery {
  def run(input: RDD[CityCountryValueSample]): RDD[CountryCityRankCompareItem] = {
    val monthsMap = Array(1, 1, 1, 1, 0, 2, 2, 2, 2, 0, 0, 0)

    val middle = input
      .filter(item =>
        (item.datetime.get(Calendar.HOUR_OF_DAY) >= 12 && item.datetime.get(Calendar.HOUR_OF_DAY) <= 15)
          && (item.datetime.get(Calendar.YEAR) == 2016 || item.datetime.get(Calendar.YEAR) == 2017)
          && monthsMap(item.datetime.get(Calendar.MONTH)) != 0
      ) // filter all unused things
      .map(item => ((item.datetime.get(Calendar.YEAR), monthsMap(item.datetime.get(Calendar.MONTH)), item.city, item.country), MeanCounter.apply(item.value))) // map to ((year, month_id, city, country), mean_counter)
      .reduceByKey(_.merge(_)) // aggregate mean counters
      .map(item => ((item._1._1, item._1._3, item._1._4), item._2.mean)) // map to ((year, city, country), mean)
      .reduceByKey((accum, value) => Math.max(accum, value) - Math.min(accum, value)) // compute differences
      .mapValues(Math.abs)

    middle.foreach(println)

    val filtered2016 = middle
      .filter(_._1._1 == 2016) // filter if year == 2016
      .map(item => (item._1._3, (item._1._2, item._2))) // map to (country, (city, value))
    val output2016 = SorterFunctions.fromPairRDD(filtered2016) // wrap in custom rdd
      .sortByKey()(Ordering.by(item => item._2)) // sort it
      .map(item => ((item._1, item._2._2._1), (item._2._1, item._2._2._2))) // map to ((country, city), (index, value))

    val filtered2017 = middle
      .filter(_._1._1 == 2017) // filter if year == 2017
      .map(item => (item._1._3, (item._1._2, item._2))) // map to (country, (city, value))
    val output2017 = SorterFunctions.fromPairRDD(filtered2017) // wrap in custom rdd
      .topByKey(3)(Ordering.by(item => item._2)) // top 3
      .map(item => ((item._1, item._2._2._1), (item._2._1, item._2._2._2))) // map to ((country, city), (index, value))

    output2017
      .join(output2016)
      .map(CountryCityRankCompareItemParser.FromTuple)
  }

}
