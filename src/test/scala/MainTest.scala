
import model.CityValueSample
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import utils.DateUtils

class MainTest extends FunSuite {
  test("Main.run") {
    val conf = new SparkConf().setMaster("local").setAppName("Main Test")
    val spark = new SparkContext(conf)

    val samples: List[CityValueSample] = List(new CityValueSample(DateUtils.parseCalendar("2012-10-01 13:00:00"), "Rome", 100))
    val rdd = spark.parallelize(samples)
    rdd.foreach(println)
  }
}
