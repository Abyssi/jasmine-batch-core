package connectors

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * CSV Reader class
  * @param _parser
  * @param classTag$O
  * @tparam O
  */
class CsvReader[O: ClassTag](var _parser: Parser[O]) extends FormatReader[O] {

  override var parser: Parser[O] = _parser

  override def load(spark: SparkContext, path: String): RDD[O] = {
    val dataset = spark.textFile(path)
    val header = dataset.first()
    dataset
      .filter(line => line != header)
      .map(item => parser.parse(item.split(",")))
  }

}
