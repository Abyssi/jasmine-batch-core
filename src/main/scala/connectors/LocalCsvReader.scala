package connectors

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LocalCsvReader {

  def load(spark: SparkContext, path: String): RDD[Array[String]] = {
    val dataset = spark.textFile(path)
    val header = dataset.first()
    dataset
      .filter(line => line != header)
      .map(_.split(","))
  }
}
