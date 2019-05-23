package queries

import java.util.Calendar

import model.{CityDescriptionSample, YearCityItem, YearCityItemParser}
import org.apache.spark.rdd.RDD

/**
  * Query 1
  * Identify the cities with clean sky, all day long,
  * for at least 15 days a month in March, April and May
  * for each year.
  **/
object ClearCitiesQuery {

  def run(input: RDD[CityDescriptionSample]): RDD[YearCityItem] = {
    val months = Array(3, 4, 5)
    val key = "sky is clear"
    input
      .filter(item => item.description.equals(key) && months.contains(item.datetime.get(Calendar.MONTH) + 1))
      .map(item => ((item.datetime.get(Calendar.YEAR), item.datetime.get(Calendar.MONTH) + 1, item.datetime.get(Calendar.DAY_OF_MONTH), item.city), 1)) //map to ((year, month, day, city), 1)
      .reduceByKey(_ + _) // sum ((year, month, day, city), counter)
      .filter(_._2 >= 20) // leave only cities with >= 20 sunny pings

      .map({ case ((year, month, _, city), _) => ((year, month, city), 1) }) //map to ((year, month, city), 1)
      .reduceByKey(_ + _) // sum ((year, month, city), counter)
      .filter(_._2 >= 15) // leave only cities with >= 15 sunny days in a month

      .map({ case ((year, _, city), _) => ((year, city), 1) })
      .reduceByKey(_ + _) // sum ((year, month, city), counter)
      .filter(_._2 == 3) // leave only cities with all 3 months sunny

      .map(item => YearCityItemParser.FromTuple(item._1)) // Map to output value (year, city)
  }

}
