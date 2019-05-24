import java.util.TimeZone

import connectors.FormatReader
import model.{CityAttributeItemParser, CityCountryValueSampleParser, _}
import org.apache.spark.{SparkConf, SparkContext}
import queries.{ClearCitiesQuery, CountryMetricsQuery, MaxDiffCountriesQuery}
import utils.Config

object Main {

  /**
    * main function
    *
    * @param args input arguments
    */
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("JASMINE Batch Core")
      .set("spark.hadoop.validateOutputSpecs", "false")

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val config = Config.parseArgs(args)

    val spark = new SparkContext(conf)

    if (config.clearCitiesQueryEnabled || config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
      val attributesInput = if (config.needJoin)
        FormatReader.apply(config.inputFormat, new CityAttributeItemParser())
          .load(spark, s"${config.inputBasePath}${config.inputFormat}/city_attributes.${config.inputFormat}") // [city, country, timeOffset]
          .map(item => (item.city, (item.country, item.timeOffset))) //map to (city, (country, timeOffset))
          .cache()
      else null

      // CLEAR CITIES QUERY
      if (config.clearCitiesQueryEnabled) {
        val weatherDescriptionInput = if (config.needJoin)
          FormatReader.apply(config.inputFormat, new CityValueItemParser())
            .load(spark, s"${config.inputBasePath}${config.inputFormat}/weather_description.${config.inputFormat}") // [datetime, city, value]
            .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
            .join(attributesInput) // join them
            .map({ case (city, ((datetime, value), (_, offset))) => CityDescriptionSample.From(datetime, offset, city, value) }) // map to [dateTime+offset, city, value]
        else
          FormatReader.apply(config.inputFormat, new CityDescriptionSampleParser())
            .load(spark, s"${config.inputBasePath}${config.inputFormat}/weather_description.${config.inputFormat}") // [datetime, city, value]

        val clearCitiesOutputPath = config.outputBasePath + "clear_cities"
        val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
        //clearCitiesOutput.foreach(println)
        clearCitiesOutput.map(_.toJsonString).saveAsTextFile(clearCitiesOutputPath)
      }

      if (config.countryMetricsQueryEnabled || config.maxDiffCountriesQueryEnabled) {
        val temperatureInput = if (config.needJoin)
          FormatReader.apply(config.inputFormat, new CityValueItemParser())
            .load(spark, s"${config.inputBasePath}${config.inputFormat}/temperature.${config.inputFormat}") // [datetime, city, value]
            .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
            .join(attributesInput) // join them
            .map({ case (city, ((datetime, value), (country, offset))) => CityCountryValueSample.From(datetime, offset, city, country, value) }) // map to [dateTime+offset, city, country, value]
            .cache()
        else
          FormatReader.apply(config.inputFormat, new CityCountryValueSampleParser())
            .load(spark, s"${config.inputBasePath}${config.inputFormat}/temperature.${config.inputFormat}") // map to [dateTime, city, country, value]
            .cache()

        // COUNTRY METRICS QUERY
        if (config.countryMetricsQueryEnabled) {
          val humidityInput = if (config.needJoin)
            FormatReader.apply(config.inputFormat, new CityValueItemParser())
              .load(spark, s"${config.inputBasePath}${config.inputFormat}/humidity.${config.inputFormat}") // [datetime, city, value]
              .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
              .join(attributesInput) // join them
              .map({ case (city, ((datetime, value), (country, offset))) => CityCountryValueSample.From(datetime, offset, city, country, value) }) // map to [dateTime+offset, city, country, value]
          else
            FormatReader.apply(config.inputFormat, new CityCountryValueSampleParser())
              .load(spark, s"${config.inputBasePath}${config.inputFormat}/humidity.${config.inputFormat}") // map to [dateTime, city, country, value]

          val pressureInput = if (config.needJoin)
            FormatReader.apply(config.inputFormat, new CityValueItemParser())
              .load(spark, s"${config.inputBasePath}${config.inputFormat}/pressure.${config.inputFormat}") // [datetime, city, value]
              .map(item => (item.city, (item.datetime, item.value))) //map to (city, (datetime, value))
              .join(attributesInput) // join them
              .map({ case (city, ((datetime, value), (country, offset))) => CityCountryValueSample.From(datetime, offset, city, country, value) }) // map to [dateTime+offset, city, country, value]
          else
            FormatReader.apply(config.inputFormat, new CityCountryValueSampleParser())
              .load(spark, s"${config.inputBasePath}${config.inputFormat}/pressure.${config.inputFormat}") // map to [dateTime, city, country, value]

          val humidityCountryMetricsOutputPath = config.outputBasePath + "humidity_country_metrics"
          val humidityCountryMetricsOutput = CountryMetricsQuery.run(humidityInput)
          humidityCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(humidityCountryMetricsOutputPath)

          val pressureCountryMetricsOutputPath = config.outputBasePath + "pressure_country_metrics"
          val pressureCountryMetricsOutput = CountryMetricsQuery.run(pressureInput)
          pressureCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(pressureCountryMetricsOutputPath)

          val temperatureCountryMetricsOutputPath = config.outputBasePath + "temperature_country_metrics"
          val temperatureCountryMetricsOutput = CountryMetricsQuery.run(temperatureInput)
          temperatureCountryMetricsOutput.map(_.toJsonString).saveAsTextFile(temperatureCountryMetricsOutputPath)
        }

        // MAX DIFF COUNTRIES QUERY
        if (config.maxDiffCountriesQueryEnabled) {
          val maxDiffCountriesOutputPath = config.outputBasePath + "max_diff_countries"
          val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
          //ProfilingUtils.timeRDD(maxDiffCountriesOutput, "max Diff Countries Output")
          maxDiffCountriesOutput.map(_.toJsonString).saveAsTextFile(maxDiffCountriesOutputPath)
        }
      }
    }

    //System.in.read
    spark.stop()
  }

}
