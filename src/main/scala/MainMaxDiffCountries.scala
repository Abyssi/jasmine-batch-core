import connectors.LocalAvroReader
import model.CityCountryValueSampleParser
import org.apache.spark.{SparkConf, SparkContext}
import queries.MaxDiffCountriesQuery
import utils.DateUtils

object MainMaxDiffCountries {

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

    val temperatureInput = new LocalAvroReader()
      .load(spark, inputBasePath + "avro/temperature.avro") // [datetime, city, value]
      .map(item => (item.get(1).toString, (item.get(0).toString, item.get(2).toString))) //map to (city, (datetime, value))
      .join(attributesInput) // join them
      .map({ case (city, ((datetime, value), (country, offset))) => (DateUtils.reformatWithTimezone(datetime, offset), city, country, value) }) // map to [dateTime+offset, city, country, value]
      .map(CityCountryValueSampleParser.FromStringTuple)
      .cache()

    val maxDiffCountriesOutputPath = outputBasePath + "max_diff_countries"
    val maxDiffCountriesOutput = MaxDiffCountriesQuery.run(temperatureInput)
    maxDiffCountriesOutput.foreach(println)
    maxDiffCountriesOutput.map(_.toJsonString).saveAsTextFile(maxDiffCountriesOutputPath)

    spark.stop()
  }

}
