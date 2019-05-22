import connectors.LocalAvroReader
import model.CityDescriptionSampleParser
import org.apache.spark.{SparkConf, SparkContext}
import queries.ClearCitiesQuery
import utils.DateUtils

object MainClearCities {

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

    val weatherDescriptionInput = new LocalAvroReader()
      .load(spark, inputBasePath + "avro/weather_description.avro") // [datetime, city, value]
      .map(item => (item.get(1).toString, (item.get(0).toString, item.get(2).toString))) //map to (city, (datetime, value))
      .join(attributesInput) // join them
      .map({ case (city, ((datetime, value), (_, offset))) => (DateUtils.reformatWithTimezone(datetime, offset), city, value) }) // map to [dateTime+offset, city, value]
      .map(CityDescriptionSampleParser.FromStringTuple)
      .cache()

    val clearCitiesOutputPath = outputBasePath + "clear_cities"
    val clearCitiesOutput = ClearCitiesQuery.run(weatherDescriptionInput)
    clearCitiesOutput.foreach(println)
    clearCitiesOutput.map(_.toJsonString).coalesce(1).saveAsTextFile(clearCitiesOutputPath)

    spark.stop()
  }

}
