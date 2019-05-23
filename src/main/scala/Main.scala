import connectors.{AvroReader, FormatReader}
import model.{CityAttributeItemParser, _}
import org.apache.spark.{SparkConf, SparkContext}
import queries.{ClearCitiesQuery, CountryMetricsQuery, MaxDiffCountriesQuery}
import utils.{Config, DateUtils}

object Main {

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

    val config = Config.parseArgs(args)
    val cityAttributeReader = FormatReader.apply(config.inputFormat, new CityAttributeItemParser())
    val cityValueReader = FormatReader.apply(config.inputFormat, new CityValueItemParser())


    val spark = new SparkContext(conf)

    if (config.clearCitiesQueryEnabled || config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
      val attributesInput = cityAttributeReader
        .load(spark, s"${config.inputBasePath}${config.inputFormat}/city_attributes.${config.inputFormat}") // [city, country, timeOffset]
        .map(item => (item.city, (item.country, item.timeOffset))) //map to (city, (country, timeOffset))
        .cache()

      // CLEAR CITIES QUERY
      if (config.clearCitiesQueryEnabled) {
        val weatherDescriptionInput = cityValueReader
          .load(spark, s"${config.inputBasePath}${config.inputFormat}/weather_description.${config.inputFormat}") // [datetime, city, value]
          .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
          .join(attributesInput) // join them
          .map({ case (city, ((datetime, value), (_, offset))) => CityDescriptionSampleParser.FromStringTuple(DateUtils.reformatWithTimezone(datetime, offset), city, value) }) // map to [dateTime+offset, city, value]
          .cache()

        val clearCitiesOutputPath = config.outputBasePath + "clear_cities"
        val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
        clearCitiesOutput.foreach(println)
        clearCitiesOutput.map(_.toJsonString).saveAsTextFile(clearCitiesOutputPath)
      }

      if (config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
        val temperatureInput = cityValueReader
          .load(spark, s"${config.inputBasePath}${config.inputFormat}/temperature.${config.inputFormat}") // [datetime, city, value]
          .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
          .join(attributesInput) // join them
          .map({ case (city, ((datetime, value), (country, offset))) => CityCountryValueSampleParser.FromStringTuple(DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
          .cache()

        // COUNTRY METRICS QUERY
        if (config.countryMetricsQueryEnabled) {
          val humidityInput = cityValueReader
            .load(spark, s"${config.inputBasePath}${config.inputFormat}/humidity.${config.inputFormat}") // [datetime, city, value]
            .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
            .join(attributesInput) // join them
            .map({ case (city, ((datetime, value), (country, offset))) => CityCountryValueSampleParser.FromStringTuple(DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
            .cache()

          val pressureInput = cityValueReader
            .load(spark, s"${config.inputBasePath}${config.inputFormat}/pressure.${config.inputFormat}") // [datetime, city, value]
            .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
            .join(attributesInput) // join them
            .map({ case (city, ((datetime, value), (country, offset))) => CityCountryValueSampleParser.FromStringTuple(DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
            .cache()

          val humidityCountryMetricsOutputPath = config.outputBasePath + "humidity_country_metrics"
          val humidityCountryMetricsOutput = CountryMetricsQuery.run(humidityInput)
          humidityCountryMetricsOutput.foreach(println)
          humidityCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(humidityCountryMetricsOutputPath)
          humidityCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(humidityCountryMetricsOutputPath)

          val pressureCountryMetricsOutputPath = config.outputBasePath + "pressure_country_metrics"
          val pressureCountryMetricsOutput = CountryMetricsQuery.run(pressureInput)
          pressureCountryMetricsOutput.foreach(println)
          pressureCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(pressureCountryMetricsOutputPath)

          val temperatureCountryMetricsOutputPath = config.outputBasePath + "temperature_country_metrics"
          val temperatureCountryMetricsOutput = CountryMetricsQuery.run(temperatureInput)
          temperatureCountryMetricsOutput.foreach(println)
          temperatureCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(temperatureCountryMetricsOutputPath)
        }

        // MAX DIFF COUNTRIES QUERY
        if (config.maxDiffCountriesQueryEnabled) {
          val maxDiffCountriesOutputPath = config.outputBasePath + "max_diff_countries"
          val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
          maxDiffCountriesOutput.foreach(println)
          maxDiffCountriesOutput.map(_.toJsonString).saveAsTextFile(maxDiffCountriesOutputPath)
        }
      }
    }

    spark.stop()
  }

}
