import connectors.LocalAvroReader
import model.CityCountryValueSampleParser
import org.apache.spark.{SparkConf, SparkContext}
import queries.CountryMetricsQuery
import utils.DateUtils

object MainCountryMetrics {

  /**
    * main function
    *
    * @param args input arguments
    */
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("JASMINE")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val inputBasePath = "data/inputs/processed/"
    val outputBasePath = "data/outputs/core/"

    val spark = new SparkContext(conf)

    val attributesInput = new LocalAvroReader()
      .load(spark, inputBasePath + "avro/city_attributes.avro") // [city, country, timeOffset]
      .map(item => (item.get(0).toString, (item.get(1).toString, item.get(2).toString)))
      .cache() //map to (city, (country, timeOffset))

    val humidityInput = new LocalAvroReader()
      .load(spark, inputBasePath + "avro/humidity.avro") // [datetime, city, value]
      .map(item => (item.get(1).toString, (item.get(0).toString, item.get(2).toString))) //map to (city, (datetime, value))
      .join(attributesInput) // join them
      .map({ case (city, ((datetime, value), (country, offset))) => (DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
      .map(CityCountryValueSampleParser.FromStringTuple)
      .cache()

    val pressureInput = new LocalAvroReader()
      .load(spark, inputBasePath + "avro/pressure.avro") // [datetime, city, value]
      .map(item => (item.get(1).toString, (item.get(0).toString, item.get(2).toString))) //map to (city, (datetime, value))
      .join(attributesInput) // join them
      .map({ case (city, ((datetime, value), (country, offset))) => (DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
      .map(CityCountryValueSampleParser.FromStringTuple)
      .cache()

    val temperatureInput = new LocalAvroReader()
      .load(spark, inputBasePath + "avro/temperature.avro") // [datetime, city, value]
      .map(item => (item.get(1).toString, (item.get(0).toString, item.get(2).toString))) //map to (city, (datetime, value))
      .join(attributesInput) // join them
      .map({ case (city, ((datetime, value), (country, offset))) => (DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
      .map(CityCountryValueSampleParser.FromStringTuple)
      .cache()

    val humidityCountryMetricsOutputPath = outputBasePath + "humidity_country_metrics"
    val humidityCountryMetricsOutput = CountryMetricsQuery.run(humidityInput)
    humidityCountryMetricsOutput.foreach(println)
    humidityCountryMetricsOutput.map(_.toJsonString).coalesce(1).saveAsTextFile(humidityCountryMetricsOutputPath)

    val pressureCountryMetricsOutputPath = outputBasePath + "pressure_country_metrics"
    val pressureCountryMetricsOutput = CountryMetricsQuery.run(pressureInput)
    pressureCountryMetricsOutput.foreach(println)
    pressureCountryMetricsOutput.map(_.toJsonString).coalesce(1).saveAsTextFile(pressureCountryMetricsOutputPath)

    val temperatureCountryMetricsOutputPath = outputBasePath + "temperature_country_metrics"
    val temperatureCountryMetricsOutput = CountryMetricsQuery.run(temperatureInput)
    temperatureCountryMetricsOutput.foreach(println)
    temperatureCountryMetricsOutput.map(_.toJsonString).coalesce(1).saveAsTextFile(temperatureCountryMetricsOutputPath)

    spark.stop()
  }

}
